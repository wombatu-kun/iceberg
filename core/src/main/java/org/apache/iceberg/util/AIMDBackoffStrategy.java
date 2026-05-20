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

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Adaptive {@link BackoffStrategy} that applies <em>Additive Increase, Multiplicative Decrease</em>
 * to the cached wait time.
 *
 * <p>Each failure ({@link #computeBackoff(int)}) atomically grows the cached delay by a fixed step,
 * capped at the configured maximum. Each success ({@link #onSuccess()}) atomically shrinks the
 * delay by a multiplicative factor, floored at the configured minimum. The strategy is intended for
 * shared use across many retried items: under sustained pressure the wait climbs linearly and stays
 * high, while a streak of successes lets it decay back toward the floor.
 *
 * <p>Returned values include up to 10% additive jitter to match {@link ExponentialBackoffStrategy}.
 *
 * <p>Configuration keys (read by {@link #initialize(Map)}):
 *
 * <ul>
 *   <li>{@code retry.aimd.min-ms} &mdash; floor for the cached delay (default 100ms).
 *   <li>{@code retry.aimd.max-ms} &mdash; cap for the cached delay (default 60000ms).
 *   <li>{@code retry.aimd.increase-step-ms} &mdash; per-failure additive step (default 1000ms).
 *   <li>{@code retry.aimd.decrease-factor} &mdash; per-success multiplicative factor in {@code (0,
 *       1]} (default 0.5; a value of 1.0 disables the decrease).
 * </ul>
 *
 * <p>Thread-safe: the cached delay is held in an {@link AtomicLong} updated via {@code
 * updateAndGet}, so concurrent {@link Tasks} workers see linearizable AI and MD transitions.
 */
public class AIMDBackoffStrategy implements BackoffStrategy {

  static final String MIN_SLEEP_TIME_MS = "retry.aimd.min-ms";
  static final String MAX_SLEEP_TIME_MS = "retry.aimd.max-ms";
  static final String INCREASE_STEP_MS = "retry.aimd.increase-step-ms";
  static final String DECREASE_FACTOR = "retry.aimd.decrease-factor";

  static final long DEFAULT_MIN_SLEEP_TIME_MS = 100L;
  static final long DEFAULT_MAX_SLEEP_TIME_MS = 60_000L;
  static final long DEFAULT_INCREASE_STEP_MS = 1_000L;
  static final double DEFAULT_DECREASE_FACTOR = 0.5;

  private long minSleepTimeMs = DEFAULT_MIN_SLEEP_TIME_MS;
  private long maxSleepTimeMs = DEFAULT_MAX_SLEEP_TIME_MS;
  private long increaseStepMs = DEFAULT_INCREASE_STEP_MS;
  private double decreaseFactor = DEFAULT_DECREASE_FACTOR;
  private final AtomicLong currentDelayMs = new AtomicLong(DEFAULT_MIN_SLEEP_TIME_MS);

  public AIMDBackoffStrategy() {}

  @Override
  public void initialize(Map<String, String> properties) {
    Map<String, String> props = properties == null ? Collections.emptyMap() : properties;

    long min = PropertyUtil.propertyAsLong(props, MIN_SLEEP_TIME_MS, DEFAULT_MIN_SLEEP_TIME_MS);
    long max = PropertyUtil.propertyAsLong(props, MAX_SLEEP_TIME_MS, DEFAULT_MAX_SLEEP_TIME_MS);
    long step = PropertyUtil.propertyAsLong(props, INCREASE_STEP_MS, DEFAULT_INCREASE_STEP_MS);
    double factor = PropertyUtil.propertyAsDouble(props, DECREASE_FACTOR, DEFAULT_DECREASE_FACTOR);

    Preconditions.checkArgument(min > 0, "%s must be > 0: %s", MIN_SLEEP_TIME_MS, min);
    Preconditions.checkArgument(
        max >= min, "%s (%s) must be >= %s (%s)", MAX_SLEEP_TIME_MS, max, MIN_SLEEP_TIME_MS, min);
    Preconditions.checkArgument(step > 0, "%s must be > 0: %s", INCREASE_STEP_MS, step);
    Preconditions.checkArgument(
        factor > 0.0 && factor <= 1.0, "%s must be in (0, 1]: %s", DECREASE_FACTOR, factor);

    this.minSleepTimeMs = min;
    this.maxSleepTimeMs = max;
    this.increaseStepMs = step;
    this.decreaseFactor = factor;
    this.currentDelayMs.set(min);
  }

  @Override
  public long computeBackoff(int attempt) {
    long delay =
        currentDelayMs.updateAndGet(current -> Math.min(maxSleepTimeMs, current + increaseStepMs));
    int jitterBound = Math.max(1, (int) (delay * 0.1));
    int jitter = ThreadLocalRandom.current().nextInt(jitterBound);
    return delay + jitter;
  }

  @Override
  public void onSuccess() {
    currentDelayMs.updateAndGet(
        current -> Math.max(minSleepTimeMs, (long) (current * decreaseFactor)));
  }

  long currentDelayMsForTesting() {
    return currentDelayMs.get();
  }
}
