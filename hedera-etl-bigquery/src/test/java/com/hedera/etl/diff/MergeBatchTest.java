package com.hedera.etl.diff;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.values.Row;
import org.junit.Rule;
import org.junit.Test;

import com.hedera.etl.util.FormatUtils;

import static com.hedera.etl.diff.Merge.LATEST;
import static com.hedera.etl.diff.Merge.UPDATED;

public class MergeBatchTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  private final String firstField = "first";
  private final String idField = "id";
  private final String secondField = "second";
  private final String actionField = "action";

  private final String timestampField = "timestamp";

  @Test
  public void testDiffMerger() {
    var schema =
        Schema.builder()
            .addInt64Field(timestampField)
            .addStringField(idField)
            .addNullableStringField(firstField)
            .addNullableStringField(secondField)
            .build();

    var diffMerger = MergeBatch.diffs(idField, timestampField);

    // given
    var input =
        pipeline
            .apply(
                Create.of(
                    getRow(schema, 1L, "1", null, "a"),
                    getRow(schema, 2L, "1", "b", "b"),
                    getRow(schema, 3L, "1", null, "c"),
                    getRow(schema, 1L, "2", null, null),
                    getRow(schema, 2L, "2", null, "b"),
                    getRow(schema, 3L, "2", null, "c"),
                    getRow(schema, 1L, "3", null, "a"),
                    getRow(schema, 2L, "3", null, ""),
                    getRow(schema, 3L, "3", null, "   "),
                    getRow(schema, 1L, "4", null, "a")))
            .setCoder(RowCoder.of(schema));

    // when
    var output = input.apply(diffMerger);

    // then
    PAssert.that(output.get(UPDATED))
        .containsInAnyOrder(
            getRow(schema, 1L, "1", null, "a"),
            getRow(schema, 2L, "1", "b", "b"),
            getRow(schema, 3L, "1", "b", "c"),
            getRow(schema, 1L, "2", null, null),
            getRow(schema, 2L, "2", null, "b"),
            getRow(schema, 3L, "2", null, "c"),
            getRow(schema, 1L, "3", null, "a"),
            getRow(schema, 2L, "3", null, "a"),
            getRow(schema, 3L, "3", null, "a"),
            getRow(schema, 1L, "4", null, "a"));

    PAssert.that(output.get(LATEST))
        .containsInAnyOrder(
            getRow(schema, 3L, "1", "b", "c"),
            getRow(schema, 3L, "2", null, "c"),
            getRow(schema, 3L, "3", null, "a"),
            getRow(schema, 1L, "4", null, "a"));

    pipeline.run();
  }

  @Test
  public void testDiffMergerWithNestedRows() {
    var fromField = "from";
    var toField = "to";
    var part1Field = "part1";
    var part2Field = "part2";

    var timestampSchema =
        Schema.builder().addNullableInt64Field(fromField).addNullableInt64Field(toField).build();

    var idSchema =
        Schema.builder()
            .addNullableStringField(part1Field)
            .addNullableStringField(part2Field)
            .build();

    var schema =
        Schema.builder()
            .addNullableRowField(timestampField, timestampSchema)
            .addNullableRowField(idField, idSchema)
            .addNullableStringField(firstField)
            .addNullableStringField(secondField)
            .build();

    var ts1 =
        Row.withSchema(timestampSchema).withFieldValues(Map.of(fromField, 1L, toField, 2L)).build();
    var ts2 =
        Row.withSchema(timestampSchema).withFieldValues(Map.of(fromField, 2L, toField, 3L)).build();
    var ts3 =
        Row.withSchema(timestampSchema).withFieldValues(Map.of(fromField, 5L, toField, 6L)).build();

    var id1 =
        Row.withSchema(idSchema).withFieldValues(Map.of(part1Field, "1", part2Field, "3")).build();
    var id2 =
        Row.withSchema(idSchema).withFieldValues(Map.of(part1Field, "2", part2Field, "5")).build();
    var id3 =
        Row.withSchema(idSchema).withFieldValues(Map.of(part1Field, "3", part2Field, "6")).build();

    // given
    var input =
        pipeline
            .apply(
                Create.of(
                    getRow(schema, ts1, id1, null, "a"),
                    getRow(schema, ts2, id1, "b", "b"),
                    getRow(schema, ts3, id1, null, "c"),
                    getRow(schema, ts1, id2, null, null),
                    getRow(schema, ts2, id2, null, "b"),
                    getRow(schema, ts3, id2, null, "c"),
                    getRow(schema, ts1, id3, null, "a")))
            .setCoder(RowCoder.of(schema));

    var diffMerger =
        MergeBatch.diffs(
            "%s.%s".formatted(idField, part1Field), "%s.%s".formatted(timestampField, toField));

    // when
    var output = input.apply(diffMerger);

    // then
    PAssert.that(output.get(UPDATED))
        .containsInAnyOrder(
            getRow(schema, ts1, id1, null, "a"),
            getRow(schema, ts2, id1, "b", "b"),
            getRow(schema, ts3, id1, "b", "c"),
            getRow(schema, ts1, id2, null, null),
            getRow(schema, ts2, id2, null, "b"),
            getRow(schema, ts3, id2, null, "c"),
            getRow(schema, ts1, id3, null, "a"));

    PAssert.that(output.get(LATEST))
        .containsInAnyOrder(
            getRow(schema, ts3, id1, "b", "c"),
            getRow(schema, ts3, id2, null, "c"),
            getRow(schema, ts1, id3, null, "a"));

    pipeline.run();
  }

  @Test
  public void testSumMerger() {
    var schema =
        Schema.builder()
            .addInt64Field(timestampField)
            .addStringField(idField)
            .addNullableDecimalField(firstField)
            .addNullableDecimalField(secondField)
            .build();

    var diffMerger = MergeBatch.sum(idField, timestampField, firstField);

    // given
    var input =
        pipeline
            .apply(
                Create.of(
                    getRow(schema, 1L, "1", BigDecimal.valueOf(1L), BigDecimal.valueOf(1L)),
                    getRow(schema, 1L, "1", BigDecimal.valueOf(1L), BigDecimal.valueOf(1L)),
                    getRow(schema, 2L, "1", BigDecimal.valueOf(2L), BigDecimal.valueOf(1L)),
                    getRow(schema, 3L, "1", BigDecimal.valueOf(3L), BigDecimal.valueOf(1L)),
                    getRow(
                        schema,
                        60_000_000_000L,
                        "1",
                        BigDecimal.valueOf(1L),
                        BigDecimal.valueOf(1L)),
                    getRow(schema, 1L, "2", BigDecimal.valueOf(10L), BigDecimal.valueOf(1L)),
                    getRow(schema, 2L, "2", BigDecimal.valueOf(-1L), BigDecimal.valueOf(1L)),
                    getRow(schema, 3L, "2", BigDecimal.valueOf(0L), BigDecimal.valueOf(1L)),
                    getRow(schema, 1L, "3", BigDecimal.valueOf(1L), BigDecimal.valueOf(1L))))
            .apply(
                WithTimestamps.of(
                    row -> FormatUtils.jodaInstantFromNanos(row.getInt64("timestamp"))))
            .setCoder(RowCoder.of(schema));

    // when
    var output = input.apply(diffMerger);

    // then
    PAssert.that(output.get(UPDATED))
        .containsInAnyOrder(
            getRow(schema, 59999000000L, "1", BigDecimal.valueOf(7L), BigDecimal.valueOf(1L)),
            getRow(schema, 119999000000L, "1", BigDecimal.valueOf(8L), BigDecimal.valueOf(1L)),
            getRow(schema, 59999000000L, "2", BigDecimal.valueOf(9L), BigDecimal.valueOf(1L)),
            getRow(schema, 59999000000L, "3", BigDecimal.valueOf(1L), BigDecimal.valueOf(1L)));

    PAssert.that(output.get(LATEST))
        .containsInAnyOrder(
            getRow(schema, 119999000000L, "1", BigDecimal.valueOf(8L), BigDecimal.valueOf(1L)),
            getRow(schema, 59999000000L, "2", BigDecimal.valueOf(9L), BigDecimal.valueOf(1L)),
            getRow(schema, 59999000000L, "3", BigDecimal.valueOf(1L), BigDecimal.valueOf(1L)));

    pipeline.run();
  }

  @Test
  public void testSumMergerWithCompoundKey() {
    var schema =
        Schema.builder()
            .addInt64Field(timestampField)
            .addStringField(idField)
            .addNullableDecimalField(firstField)
            .addStringField(secondField)
            .build();

    var diffMerger = MergeBatch.sum(List.of(idField, secondField), timestampField, firstField);

    // given
    var input =
        pipeline
            .apply(
                Create.of(
                    getRow(schema, 1L, "1", BigDecimal.valueOf(1L), "1"),
                    getRow(schema, 1L, "1", BigDecimal.valueOf(1L), "2"),
                    getRow(schema, 2L, "1", BigDecimal.valueOf(2L), "1"),
                    getRow(schema, 3L, "1", BigDecimal.valueOf(3L), "2"),
                    getRow(schema, 60_000_000_000L, "1", BigDecimal.valueOf(1L), "1"),
                    getRow(schema, 1L, "1", BigDecimal.valueOf(10L), "2"),
                    getRow(schema, 2L, "1", BigDecimal.valueOf(-1L), "1"),
                    getRow(schema, 3L, "1", BigDecimal.valueOf(0L), "2"),
                    getRow(schema, 1L, "1", BigDecimal.valueOf(1L), "1")))
            .apply(
                WithTimestamps.of(
                    row -> FormatUtils.jodaInstantFromNanos(row.getInt64("timestamp"))))
            .setCoder(RowCoder.of(schema));

    // when
    var output = input.apply(diffMerger);

    // then
    PAssert.that(output.get(UPDATED))
        .containsInAnyOrder(
            getRow(schema, 59999000000L, "1", BigDecimal.valueOf(3L), "1"),
            getRow(schema, 119999000000L, "1", BigDecimal.valueOf(4L), "1"),
            getRow(schema, 59999000000L, "1", BigDecimal.valueOf(14L), "2"));

    PAssert.that(output.get(LATEST))
        .containsInAnyOrder(
            getRow(schema, 119999000000L, "1", BigDecimal.valueOf(4L), "1"),
            getRow(schema, 59999000000L, "1", BigDecimal.valueOf(14L), "2"));

    pipeline.run();
  }

  @Test
  public void testAppendMerger() {
    var schema =
        Schema.builder()
            .addInt64Field(timestampField)
            .addStringField(idField)
            .addNullableByteArrayField(firstField)
            .addNullableByteArrayField(secondField)
            .addStringField(actionField)
            .build();

    var diffMerger = MergeBatch.append(idField, timestampField, firstField, actionField);

    // given
    var input =
        pipeline
            .apply(
                Create.of(
                    getRow(schema, 1L, "1", new byte[] {0, 1}, new byte[] {2}, "create"),
                    getRow(schema, 2L, "1", new byte[] {2}, new byte[] {3}, "update"),
                    getRow(schema, 3L, "1", null, null, "update"),
                    getRow(schema, 4L, "1", null, null, "delete"),
                    getRow(schema, 1L, "2", null, new byte[] {0, 1, 2, 3}, "create"),
                    getRow(schema, 2L, "2", new byte[] {0, 1, 2, 3}, null, "update"),
                    getRow(schema, 3L, "2", null, null, "update"),
                    getRow(schema, 1L, "3", new byte[] {0, 1}, new byte[] {0, 1}, "create")))
            .setCoder(RowCoder.of(schema));

    // when
    var output = input.apply(diffMerger);

    // then
    PAssert.that(output.get(UPDATED))
        .containsInAnyOrder(
            getRow(schema, 1L, "1", new byte[] {0, 1}, new byte[] {2}, "create"),
            getRow(schema, 2L, "1", new byte[] {0, 1, 2}, new byte[] {3}, "update"),
            getRow(schema, 3L, "1", new byte[] {0, 1, 2}, new byte[] {3}, "update"),
            getRow(schema, 4L, "1", null, new byte[] {3}, "delete"),
            getRow(schema, 1L, "2", null, new byte[] {0, 1, 2, 3}, "create"),
            getRow(schema, 2L, "2", new byte[] {0, 1, 2, 3}, new byte[] {0, 1, 2, 3}, "update"),
            getRow(schema, 3L, "2", new byte[] {0, 1, 2, 3}, new byte[] {0, 1, 2, 3}, "update"),
            getRow(schema, 1L, "3", new byte[] {0, 1}, new byte[] {0, 1}, "create"));

    PAssert.that(output.get(LATEST))
        .containsInAnyOrder(
            getRow(schema, 4L, "1", null, new byte[] {3}, "delete"),
            getRow(schema, 3L, "2", new byte[] {0, 1, 2, 3}, new byte[] {0, 1, 2, 3}, "update"),
            getRow(schema, 1L, "3", new byte[] {0, 1}, new byte[] {0, 1}, "create"));

    pipeline.run();
  }

  private Row getRow(Schema schema, Object timestamp, Object id, Object first, Object second) {
    return Row.withSchema(schema)
        .withFieldValue(timestampField, timestamp)
        .withFieldValue(idField, id)
        .withFieldValue(firstField, first)
        .withFieldValue(secondField, second)
        .build();
  }

  private Row getRow(
      Schema schema, Object timestamp, Object id, Object first, Object second, String action) {
    return Row.withSchema(schema)
        .withFieldValue(timestampField, timestamp)
        .withFieldValue(idField, id)
        .withFieldValue(firstField, first)
        .withFieldValue(secondField, second)
        .withFieldValue(actionField, action)
        .build();
  }
}
