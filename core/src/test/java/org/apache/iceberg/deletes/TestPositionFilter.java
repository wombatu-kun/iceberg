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
package org.apache.iceberg.deletes;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.function.Predicate;
import org.apache.avro.util.Utf8;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

public class TestPositionFilter {
  @Test
  public void testPositionFileFilter() {
    List<StructLike> positionDeletes =
        Lists.newArrayList(
            Row.of("file_a.avro", 0L),
            Row.of("file_a.avro", 3L),
            Row.of(new Utf8("file_a.avro"), 9L),
            Row.of("file_a.avro", 22L),
            Row.of("file_a.avro", 56L),
            Row.of(new Utf8("file_b.avro"), 16L),
            Row.of("file_b.avro", 19L),
            Row.of("file_b.avro", 63L),
            Row.of("file_b.avro", 70L),
            Row.of("file_b.avro", 91L));

    assertThat(
            Deletes.deletePositions(
                "file_a.avro", CloseableIterable.withNoopClose(positionDeletes)))
        .as("Should contain only file_a positions")
        .containsExactly(0L, 3L, 9L, 22L, 56L);

    assertThat(
            Deletes.deletePositions(
                "file_b.avro", CloseableIterable.withNoopClose(positionDeletes)))
        .as("Should contain only file_b positions")
        .containsExactly(16L, 19L, 63L, 70L, 91L);

    assertThat(
            Deletes.deletePositions(
                "file_c.avro", CloseableIterable.withNoopClose(positionDeletes)))
        .as("Should contain no positions for file_c")
        .isEmpty();
  }

  @Test
  public void testPositionMerging() {
    List<StructLike> positionDeletes1 =
        Lists.newArrayList(
            Row.of("file_a.avro", 0L),
            Row.of("file_a.avro", 3L),
            Row.of("file_a.avro", 9L),
            Row.of("file_a.avro", 22L),
            Row.of("file_a.avro", 56L));

    List<StructLike> positionDeletes2 =
        Lists.newArrayList(
            Row.of("file_a.avro", 16L),
            Row.of("file_a.avro", 19L),
            Row.of("file_a.avro", 63L),
            Row.of("file_a.avro", 70L),
            Row.of("file_a.avro", 91L));

    List<StructLike> positionDeletes3 =
        Lists.newArrayList(
            Row.of("file_a.avro", 3L), Row.of("file_a.avro", 19L), Row.of("file_a.avro", 22L));

    List<CloseableIterable<StructLike>> deletes =
        Lists.newArrayList(
            CloseableIterable.withNoopClose(positionDeletes1),
            CloseableIterable.withNoopClose(positionDeletes2),
            CloseableIterable.withNoopClose(positionDeletes3));

    assertThat(Deletes.deletePositions("file_a.avro", deletes))
        .as("Should merge deletes in order, with duplicates")
        .containsExactly(0L, 3L, 3L, 9L, 16L, 19L, 19L, 22L, 22L, 56L, 63L, 70L, 91L);
  }

  @Test
  public void testPositionSetRowFilter() {
    CloseableIterable<StructLike> rows =
        CloseableIterable.withNoopClose(
            Lists.newArrayList(
                Row.of(0L, "a"),
                Row.of(1L, "b"),
                Row.of(2L, "c"),
                Row.of(3L, "d"),
                Row.of(4L, "e"),
                Row.of(5L, "f"),
                Row.of(6L, "g"),
                Row.of(7L, "h"),
                Row.of(8L, "i"),
                Row.of(9L, "j")));

    CloseableIterable<Long> deletes =
        CloseableIterable.withNoopClose(Lists.newArrayList(0L, 3L, 4L, 7L, 9L));

    Predicate<StructLike> shouldKeep =
        row -> !Deletes.toPositionIndex(deletes).isDeleted(row.get(0, Long.class));
    CloseableIterable<StructLike> actual = CloseableIterable.filter(rows, shouldKeep);

    assertThat(Iterables.transform(actual, row -> row.get(0, Long.class)))
        .as("Filter should produce expected rows")
        .containsExactlyElementsOf(Lists.newArrayList(1L, 2L, 5L, 6L, 8L));
  }

  @Test
  public void testCombinedPositionSetRowFilter() {
    CloseableIterable<StructLike> positionDeletes1 =
        CloseableIterable.withNoopClose(
            Lists.newArrayList(
                Row.of("file_a.avro", 0L),
                Row.of("file_a.avro", 3L),
                Row.of("file_a.avro", 9L),
                Row.of("file_b.avro", 5L),
                Row.of("file_b.avro", 6L)));

    CloseableIterable<StructLike> positionDeletes2 =
        CloseableIterable.withNoopClose(
            Lists.newArrayList(
                Row.of("file_a.avro", 3L),
                Row.of("file_a.avro", 4L),
                Row.of("file_a.avro", 7L),
                Row.of("file_b.avro", 2L)));

    CloseableIterable<StructLike> rows =
        CloseableIterable.withNoopClose(
            Lists.newArrayList(
                Row.of(0L, "a"),
                Row.of(1L, "b"),
                Row.of(2L, "c"),
                Row.of(3L, "d"),
                Row.of(4L, "e"),
                Row.of(5L, "f"),
                Row.of(6L, "g"),
                Row.of(7L, "h"),
                Row.of(8L, "i"),
                Row.of(9L, "j")));

    CloseableIterable<Long> positions =
        CloseableIterable.transform(
            CloseableIterable.filter(
                CloseableIterable.concat(ImmutableList.of(positionDeletes1, positionDeletes2)),
                row -> "file_a.avro".equals(row.get(0, String.class))),
            row -> row.get(1, Long.class));
    Predicate<StructLike> isDeleted =
        row -> Deletes.toPositionIndex(positions).isDeleted(row.get(0, Long.class));
    CloseableIterable<StructLike> actual = CloseableIterable.filter(rows, isDeleted.negate());

    assertThat(Iterables.transform(actual, row -> row.get(0, Long.class)))
        .as("Filter should produce expected rows")
        .containsExactlyElementsOf(Lists.newArrayList(1L, 2L, 5L, 6L, 8L));
  }
}
