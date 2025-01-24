package com.hedera.etl.diff;

import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.Row;
import org.junit.Rule;
import org.junit.Test;
import java.util.Map;

public class MergeTest {
  private final String timestampField = "timestamp";
  private final String idField = "id";
  private final String firstField = "first";
  private final String secondField = "second";

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testDiffMerger() {
    var schema = Schema.builder()
            .addInt64Field(timestampField)
            .addStringField(idField)
            .addNullableStringField(firstField)
            .addNullableStringField(secondField)
            .build();

    var diffMerger = Merge.diffs(idField, timestampField);

    // given
    var input = pipeline.apply(Create.of(
            getRow(schema, 1L, "1", null, "a"),
            getRow(schema, 2L, "1", "b", "b"),
            getRow(schema, 3L, "1", null, "c"),
            getRow(schema, 1L, "2", null, null),
            getRow(schema, 2L, "2", null, "b"),
            getRow(schema, 3L, "2", null, "c"),
            getRow(schema, 1L, "3", null, "a")
    )).setCoder(RowCoder.of(schema));

    // when
    var output = input.apply(diffMerger);

    // then
    PAssert.that(output.get(Merge.UPDATED)).containsInAnyOrder(
            getRow(schema, 1L, "1", null, "a"),
            getRow(schema, 2L, "1", "b", "b"),
            getRow(schema, 3L, "1", "b", "c"),
            getRow(schema, 1L, "2", null, null),
            getRow(schema, 2L, "2", null, "b"),
            getRow(schema, 3L, "2", null, "c"),
            getRow(schema, 1L, "3", null, "a")
    );

    PAssert.that(output.get(Merge.LATEST)).containsInAnyOrder(
            getRow(schema, 3L, "1", "b", "c"),
            getRow(schema, 3L, "2", null, "c"),
            getRow(schema, 1L, "3", null, "a")
    );

    pipeline.run();
  }

  @Test
  public void testDiffMergerWithNestedRows() {
    var fromField = "from";
    var toField = "to";
    var part1Field = "part1";
    var part2Field = "part2";

    var timestampSchema = Schema.builder()
            .addNullableInt64Field(fromField)
            .addNullableInt64Field(toField)
            .build();

    var idSchema = Schema.builder()
            .addNullableStringField(part1Field)
            .addNullableStringField(part2Field)
            .build();

    var schema = Schema.builder()
            .addNullableRowField(timestampField, timestampSchema)
            .addNullableRowField(idField, idSchema)
            .addNullableStringField(firstField)
            .addNullableStringField(secondField)
            .build();

    var ts1 = Row.withSchema(timestampSchema).withFieldValues(Map.of(fromField, 1L, toField, 2L)).build();
    var ts2 = Row.withSchema(timestampSchema).withFieldValues(Map.of(fromField, 2L, toField, 3L)).build();
    var ts3 = Row.withSchema(timestampSchema).withFieldValues(Map.of(fromField, 5L, toField, 6L)).build();

    var id1 = Row.withSchema(idSchema).withFieldValues(Map.of(part1Field, "1", part2Field, "3")).build();
    var id2 = Row.withSchema(idSchema).withFieldValues(Map.of(part1Field, "2", part2Field, "5")).build();
    var id3 = Row.withSchema(idSchema).withFieldValues(Map.of(part1Field, "3", part2Field, "6")).build();

    // given
    var input = pipeline.apply(Create.of(
            getRow(schema, ts1, id1, null, "a"),
            getRow(schema, ts2, id1, "b", "b"),
            getRow(schema, ts3, id1, null, "c"),
            getRow(schema, ts1, id2, null, null),
            getRow(schema, ts2, id2, null, "b"),
            getRow(schema, ts3, id2, null, "c"),
            getRow(schema, ts1, id3, null, "a")
    )).setCoder(RowCoder.of(schema));

    var diffMerger = Merge.diffs("%s.%s".formatted(idField, part1Field), "%s.%s".formatted(timestampField, toField));

    // when
    var output = input.apply(diffMerger);

    // then
    PAssert.that(output.get(Merge.UPDATED)).containsInAnyOrder(
            getRow(schema, ts1, id1, null, "a"),
            getRow(schema, ts2, id1, "b", "b"),
            getRow(schema, ts3, id1, "b", "c"),
            getRow(schema, ts1, id2, null, null),
            getRow(schema, ts2, id2, null, "b"),
            getRow(schema, ts3, id2, null, "c"),
            getRow(schema, ts1, id3, null, "a")
    );

    PAssert.that(output.get(Merge.LATEST)).containsInAnyOrder(
            getRow(schema, ts3, id1, "b", "c"),
            getRow(schema, ts3, id2, null, "c"),
            getRow(schema, ts1, id3, null, "a")
    );

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

  @Test
  public void testSumMerger() {
    var schema = Schema.builder()
            .addInt64Field(timestampField)
            .addStringField(idField)
            .addNullableInt64Field(firstField)
            .addNullableInt64Field(secondField)
            .build();

    var diffMerger = Merge.sum(idField, timestampField, firstField);

    // given
    var input = pipeline.apply(Create.of(
            getRow(schema, 1L, "1", 1L, 1L),
            getRow(schema, 2L, "1", 2L, 2L),
            getRow(schema, 3L, "1", 3L, 3L),
            getRow(schema, 1L, "2", 10L, 4L),
            getRow(schema, 2L, "2", -1L, 5L),
            getRow(schema, 3L, "2", 0L, 6L),
            getRow(schema, 1L, "3", 1L, 7L)
    )).setCoder(RowCoder.of(schema));

    // when
    var output = input.apply(diffMerger);

    // then
    PAssert.that(output.get(Merge.UPDATED)).containsInAnyOrder(
            getRow(schema, 1L, "1", 1L, 1L),
            getRow(schema, 2L, "1", 3L, 2L),
            getRow(schema, 3L, "1", 6L, 3L),
            getRow(schema, 1L, "2", 10L, 4L),
            getRow(schema, 2L, "2", 9L, 5L),
            getRow(schema, 3L, "2", 9L, 6L),
            getRow(schema, 1L, "3", 1L, 7L)
    );

    PAssert.that(output.get(Merge.LATEST)).containsInAnyOrder(
            getRow(schema, 3L, "1", 6L, 3L),
            getRow(schema, 3L, "2", 9L, 6L),
            getRow(schema, 1L, "3", 1L, 7L)
    );

    pipeline.run();
  }
}
