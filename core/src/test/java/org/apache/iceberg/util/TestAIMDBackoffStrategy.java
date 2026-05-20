/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

public class TestAIMDBackoffStrategy {

  @Test
  public void defaultsExposedForReuse() {
    assertThat(AIMDBackoffStrategy.DEFAULT_MIN_SLEEP_TIME_MS).isEqualTo(100L);
    assertThat(AIMDBackoffStrategy.DEFAULT_MAX_SLEEP_TIME_MS).isEqualTo(60_000L);
    assertThat(AIMDBackoffStrategy.DEFAULT_INCREASE_STEP_MS).isEqualTo(1_000L);
    assertThat(AIMDBackoffStrategy.DEFAULT_DECREASE_FACTOR).isEqualTo(0.5);
  }

  @Test
  public void firstFailureReturnsMinPlusStep() {
    AIMDBackoffStrategy backoff = newStrategy(100, 60_000, 1_000, 0.5);

    long wait = backoff.computeBackoff(1);

    long expectedBase = 100 + 1_000;
    long jitterBound = Math.max(1, (long) (expectedBase * 0.1));
    assertThat(wait).isGreaterThanOrEqualTo(expectedBase).isLessThan(expectedBase + jitterBound);
    assertThat(backoff.currentDelayMsForTesting()).isEqualTo(expectedBase);
  }

  @Test
  public void additiveIncreaseGrowsLinearly() {
    long min = 100;
    long step = 1_000;
    AIMDBackoffStrategy backoff = newStrategy(min, 60_000, step, 0.5);

    for (int i = 1; i <= 5; i++) {
      long expectedBase = min + i * step;
      long wait = backoff.computeBackoff(i);
      long jitterBound = Math.max(1, (long) (expectedBase * 0.1));
      assertThat(wait).isGreaterThanOrEqualTo(expectedBase).isLessThan(expectedBase + jitterBound);
      assertThat(backoff.currentDelayMsForTesting()).isEqualTo(expectedBase);
    }
  }

  @Test
  public void additiveIncreaseRespectsMaxCap() {
    long max = 5_000;
    AIMDBackoffStrategy backoff = newStrategy(100, max, 1_000, 0.5);

    for (int i = 1; i <= 50; i++) {
      backoff.computeBackoff(i);
    }

    assertThat(backoff.currentDelayMsForTesting()).isEqualTo(max);

    long jitterBound = Math.max(1, (long) (max * 0.1));
    long wait = backoff.computeBackoff(51);
    assertThat(wait).isGreaterThanOrEqualTo(max).isLessThan(max + jitterBound);
  }

  @Test
  public void onSuccessAppliesMultiplicativeDecrease() {
    AIMDBackoffStrategy backoff = newStrategy(100, 60_000, 1_000, 0.5);

    for (int i = 1; i <= 4; i++) {
      backoff.computeBackoff(i);
    }
    long beforeSuccess = backoff.currentDelayMsForTesting();
    assertThat(beforeSuccess).isEqualTo(100 + 4 * 1_000);

    backoff.onSuccess();

    assertThat(backoff.currentDelayMsForTesting()).isEqualTo((long) (beforeSuccess * 0.5));
  }

  @Test
  public void onSuccessClampsToMin() {
    long min = 100;
    AIMDBackoffStrategy backoff = newStrategy(min, 60_000, 1_000, 0.5);

    backoff.computeBackoff(1);
    backoff.computeBackoff(2);
    for (int i = 0; i < 50; i++) {
      backoff.onSuccess();
    }

    assertThat(backoff.currentDelayMsForTesting()).isEqualTo(min);
  }

  @Test
  public void onSuccessFromInitialStateIsNoOp() {
    long min = 100;
    AIMDBackoffStrategy backoff = newStrategy(min, 60_000, 1_000, 0.5);

    backoff.onSuccess();
    backoff.onSuccess();
    backoff.onSuccess();

    assertThat(backoff.currentDelayMsForTesting()).isEqualTo(min);
  }

  @Test
  public void decreaseFactorOneDisablesMultiplicativeDecrease() {
    AIMDBackoffStrategy backoff = newStrategy(100, 60_000, 1_000, 1.0);

    backoff.computeBackoff(1);
    backoff.computeBackoff(2);
    long before = backoff.currentDelayMsForTesting();

    backoff.onSuccess();

    assertThat(backoff.currentDelayMsForTesting()).isEqualTo(before);
  }

  @Test
  public void jitterIsAdditiveAndBoundedAroundBase() {
    // min == max forces the cached delay to stay at the cap, so every computeBackoff call
    // returns base + jitter and never advances state.
    long base = 1_000;
    AIMDBackoffStrategy steady = newStrategy(base, base, 1, 0.5);
    long jitterBound = Math.max(1, (long) (base * 0.1));

    for (int i = 0; i < 200; i++) {
      long wait = steady.computeBackoff(1);
      assertThat(wait).isGreaterThanOrEqualTo(base).isLessThan(base + jitterBound);
    }
  }

  @Test
  public void concurrentFailuresAccumulateAtomically() throws Exception {
    long min = 100;
    long step = 1_000;
    int threads = 8;
    AIMDBackoffStrategy backoff =
        newStrategy(min, /* large enough to avoid saturation */ Long.MAX_VALUE, step, 0.5);

    runConcurrently(threads, latch -> () -> awaitAndRun(latch, () -> backoff.computeBackoff(1)));

    assertThat(backoff.currentDelayMsForTesting()).isEqualTo(min + threads * step);
  }

  @Test
  public void concurrentSuccessesAccumulateAtomically() throws Exception {
    // min=1, max=1024, step=1023, factor=0.5 lets one failure jump to max and exactly 10
    // successes collapse back to min (1024 * 0.5^10 = 1)
    AIMDBackoffStrategy backoff = newStrategy(1, 1_024, 1_023, 0.5);
    backoff.computeBackoff(1);
    assertThat(backoff.currentDelayMsForTesting()).isEqualTo(1_024L);

    int threads = 10;
    runConcurrently(threads, latch -> () -> awaitAndRun(latch, backoff::onSuccess));

    assertThat(backoff.currentDelayMsForTesting()).isEqualTo(1L);
  }

  @Test
  public void initializeRejectsNonPositiveMin() {
    assertThatThrownBy(
            () ->
                new AIMDBackoffStrategy()
                    .initialize(ImmutableMap.of(AIMDBackoffStrategy.MIN_SLEEP_TIME_MS, "0")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(AIMDBackoffStrategy.MIN_SLEEP_TIME_MS);
  }

  @Test
  public void initializeRejectsMaxBelowMin() {
    assertThatThrownBy(
            () ->
                new AIMDBackoffStrategy()
                    .initialize(
                        ImmutableMap.of(
                            AIMDBackoffStrategy.MIN_SLEEP_TIME_MS, "500",
                            AIMDBackoffStrategy.MAX_SLEEP_TIME_MS, "100")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(AIMDBackoffStrategy.MAX_SLEEP_TIME_MS);
  }

  @Test
  public void initializeRejectsNonPositiveStep() {
    assertThatThrownBy(
            () ->
                new AIMDBackoffStrategy()
                    .initialize(ImmutableMap.of(AIMDBackoffStrategy.INCREASE_STEP_MS, "0")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(AIMDBackoffStrategy.INCREASE_STEP_MS);
  }

  @Test
  public void initializeRejectsOutOfRangeFactor() {
    assertThatThrownBy(
            () ->
                new AIMDBackoffStrategy()
                    .initialize(ImmutableMap.of(AIMDBackoffStrategy.DECREASE_FACTOR, "0")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(AIMDBackoffStrategy.DECREASE_FACTOR);

    assertThatThrownBy(
            () ->
                new AIMDBackoffStrategy()
                    .initialize(ImmutableMap.of(AIMDBackoffStrategy.DECREASE_FACTOR, "1.5")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(AIMDBackoffStrategy.DECREASE_FACTOR);
  }

  @Test
  public void initializeAcceptsNullProperties() {
    AIMDBackoffStrategy backoff = new AIMDBackoffStrategy();

    backoff.initialize(null);

    assertThat(backoff.currentDelayMsForTesting())
        .isEqualTo(AIMDBackoffStrategy.DEFAULT_MIN_SLEEP_TIME_MS);
  }

  @Test
  public void loadsViaBackoffStrategiesFactory() {
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put(BackoffStrategies.STRATEGY_IMPL, AIMDBackoffStrategy.class.getName())
            .put(AIMDBackoffStrategy.MIN_SLEEP_TIME_MS, "200")
            .put(AIMDBackoffStrategy.MAX_SLEEP_TIME_MS, "10000")
            .put(AIMDBackoffStrategy.INCREASE_STEP_MS, "500")
            .put(AIMDBackoffStrategy.DECREASE_FACTOR, "0.25")
            .build();

    BackoffStrategy strategy = BackoffStrategies.from(properties);

    assertThat(strategy).isInstanceOf(AIMDBackoffStrategy.class);
    AIMDBackoffStrategy aimd = (AIMDBackoffStrategy) strategy;
    long wait = aimd.computeBackoff(1);
    long expectedBase = 200 + 500;
    long jitterBound = Math.max(1, (long) (expectedBase * 0.1));
    assertThat(wait).isGreaterThanOrEqualTo(expectedBase).isLessThan(expectedBase + jitterBound);
    assertThat(aimd.currentDelayMsForTesting()).isEqualTo(expectedBase);
  }

  private static AIMDBackoffStrategy newStrategy(long min, long max, long step, double factor) {
    AIMDBackoffStrategy strategy = new AIMDBackoffStrategy();
    strategy.initialize(
        ImmutableMap.of(
            AIMDBackoffStrategy.MIN_SLEEP_TIME_MS, Long.toString(min),
            AIMDBackoffStrategy.MAX_SLEEP_TIME_MS, Long.toString(max),
            AIMDBackoffStrategy.INCREASE_STEP_MS, Long.toString(step),
            AIMDBackoffStrategy.DECREASE_FACTOR, Double.toString(factor)));
    return strategy;
  }

  private interface LatchedTaskFactory {
    Runnable build(CountDownLatch latch);
  }

  private static void runConcurrently(int threads, LatchedTaskFactory factory) throws Exception {
    ExecutorService svc = Executors.newFixedThreadPool(threads);
    try {
      CountDownLatch start = new CountDownLatch(1);
      CountDownLatch done = new CountDownLatch(threads);
      for (int i = 0; i < threads; i++) {
        Runnable body = factory.build(start);
        svc.submit(
            () -> {
              try {
                body.run();
              } finally {
                done.countDown();
              }
            });
      }
      start.countDown();
      assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
    } finally {
      svc.shutdownNow();
      assertThat(svc.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    }
  }

  private static void awaitAndRun(CountDownLatch latch, Runnable body) {
    try {
      latch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
    body.run();
  }
}
