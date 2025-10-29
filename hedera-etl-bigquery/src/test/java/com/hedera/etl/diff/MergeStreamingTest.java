package com.hedera.etl.diff;

import java.math.BigDecimal;

import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;

import com.hedera.etl.util.FormatUtils;

import static com.hedera.etl.diff.Merge.UPDATED;

public class MergeStreamingTest {
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

    var diffMerger = MergeStreaming.diffs(idField, timestampField);

    // given
    var createInput =
        TestStream.create(SchemaCoder.of(schema))
            .advanceWatermarkTo(new Instant(0L).plus(Duration.standardMinutes(1)))
            .addElements(
                getRow(schema, 1L, "1", null, "a"),
                getRow(schema, 1L, "2", null, null),
                getRow(schema, 1L, "3", null, "a"),
                getRow(schema, 1L, "4", null, "a"))
            .advanceWatermarkTo(new Instant(0L).plus(Duration.standardMinutes(2)))
            .addElements(
                getRow(schema, 2L, "1", "b", "b"),
                getRow(schema, 2L, "2", null, "b"),
                getRow(schema, 2L, "3", null, ""))
            .advanceWatermarkTo(new Instant(0L).plus(Duration.standardMinutes(3)))
            .addElements(
                getRow(schema, 3L, "1", null, "c"),
                getRow(schema, 3L, "2", null, "c"),
                getRow(schema, 3L, "3", null, "   "))
            .advanceWatermarkTo(new Instant(0L).plus(Duration.standardDays(1)))
            .addElements(getRow(schema, 86_400_000_000_003L, "1", "d", "c"))
            .advanceWatermarkToInfinity();

    var input = pipeline.apply(createInput).setCoder(RowCoder.of(schema));

    // when
    var output = input.apply(diffMerger);

    PAssert.that(output.get(UPDATED))
        .containsInAnyOrder(
            getRow(schema, 1L, "1", null, "a"),
            getRow(schema, 1L, "2", null, null),
            getRow(schema, 1L, "3", null, "a"),
            getRow(schema, 1L, "4", null, "a"),
            getRow(schema, 2L, "1", "b", "b"),
            getRow(schema, 2L, "2", null, "b"),
            getRow(schema, 2L, "3", null, "a"),
            getRow(schema, 3L, "1", "b", "c"),
            getRow(schema, 3L, "2", null, "c"),
            getRow(schema, 3L, "3", null, "a"),
            getRow(schema, 86_400_000_000_003L, "1", "d", "c"));
    // then

    PAssert.that(output.get(Merge.LATEST))
        .containsInAnyOrder(
            getRow(schema, 3L, "1", "b", "c"),
            getRow(schema, 3L, "2", null, "c"),
            getRow(schema, 3L, "3", null, "a"),
            getRow(schema, 1L, "4", null, "a"),
            getRow(schema, 86_400_000_000_003L, "1", "d", "c"));

    pipeline.run();
  }

  @Test
  public void testSumMerger() {
    final var nanosInMinute = 60_000_000_000L;

    var schema =
        Schema.builder()
            .addInt64Field(timestampField)
            .addStringField(idField)
            .addNullableDecimalField(firstField)
            .addNullableDecimalField(secondField)
            .build();

    var diffMerger = MergeStreaming.sum(idField, timestampField, firstField);

    // given
    var createInput =
        TestStream.create(SchemaCoder.of(schema))
            .advanceWatermarkTo(new Instant(0L).plus(Duration.standardMinutes(1)))
            .addElements(
                getRow(
                    schema, 1 * nanosInMinute, "1", BigDecimal.valueOf(1L), BigDecimal.valueOf(1L)),
                getRow(
                    schema, 1 * nanosInMinute, "1", BigDecimal.valueOf(1L), BigDecimal.valueOf(1L)),
                getRow(
                    schema,
                    1 * nanosInMinute,
                    "2",
                    BigDecimal.valueOf(10L),
                    BigDecimal.valueOf(1L)),
                getRow(
                    schema, 1 * nanosInMinute, "3", BigDecimal.valueOf(1L), BigDecimal.valueOf(1L)))
            .advanceWatermarkTo(new Instant(0L).plus(Duration.standardMinutes(2)))
            .addElements(
                getRow(
                    schema, 2 * nanosInMinute, "1", BigDecimal.valueOf(2L), BigDecimal.valueOf(1L)),
                getRow(
                    schema,
                    2 * nanosInMinute,
                    "2",
                    BigDecimal.valueOf(-1L),
                    BigDecimal.valueOf(1L)))
            .advanceWatermarkTo(new Instant(0L).plus(Duration.standardMinutes(3)))
            .addElements(
                getRow(
                    schema, 3 * nanosInMinute, "1", BigDecimal.valueOf(3L), BigDecimal.valueOf(1L)),
                getRow(
                    schema, 3 * nanosInMinute, "2", BigDecimal.valueOf(0L), BigDecimal.valueOf(1L)))
            .advanceWatermarkTo(new Instant(0L).plus(Duration.standardMinutes(10)))
            .addElements(
                getRow(
                    schema,
                    10 * nanosInMinute,
                    "1",
                    BigDecimal.valueOf(1L),
                    BigDecimal.valueOf(1L)))
            .advanceWatermarkToInfinity();

    var input =
        pipeline
            .apply(createInput)
            .apply(
                WithTimestamps.of(
                    row -> FormatUtils.jodaInstantFromNanos(row.getInt64("timestamp"))))
            .setCoder(RowCoder.of(schema));

    // when
    var output = input.apply(diffMerger);

    // then
    PAssert.that(output.get(UPDATED))
        .containsInAnyOrder(
            getRow(schema, 119999000000L, "1", BigDecimal.valueOf(2L), BigDecimal.valueOf(1L)),
            getRow(schema, 179999000000L, "1", BigDecimal.valueOf(4L), BigDecimal.valueOf(1L)),
            getRow(schema, 239999000000L, "1", BigDecimal.valueOf(7L), BigDecimal.valueOf(1L)),
            getRow(schema, 659999000000L, "1", BigDecimal.valueOf(8L), BigDecimal.valueOf(1L)),
            getRow(schema, 119999000000L, "2", BigDecimal.valueOf(10L), BigDecimal.valueOf(1L)),
            getRow(schema, 179999000000L, "2", BigDecimal.valueOf(9L), BigDecimal.valueOf(1L)),
            getRow(schema, 239999000000L, "2", BigDecimal.valueOf(9L), BigDecimal.valueOf(1L)),
            getRow(schema, 119999000000L, "3", BigDecimal.valueOf(1L), BigDecimal.valueOf(1L)));

    PAssert.that(output.get(Merge.LATEST))
        .containsInAnyOrder(
            getRow(schema, 659999000000L, "1", BigDecimal.valueOf(8L), BigDecimal.valueOf(1L)),
            getRow(schema, 239999000000L, "2", BigDecimal.valueOf(9L), BigDecimal.valueOf(1L)),
            getRow(schema, 119999000000L, "3", BigDecimal.valueOf(1L), BigDecimal.valueOf(1L)));

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

    var diffMerger = MergeStreaming.append(idField, timestampField, firstField, actionField);

    // given
    var createInput =
        TestStream.create(SchemaCoder.of(schema))
            .advanceWatermarkTo(new Instant(0L).plus(Duration.standardMinutes(1)))
            .addElements(
                getRow(schema, 1L, "1", new byte[] {0, 1}, new byte[] {2}, "create"),
                getRow(schema, 1L, "2", null, new byte[] {0, 1, 2, 3}, "create"),
                getRow(schema, 1L, "3", new byte[] {0, 1}, new byte[] {0, 1}, "create"))
            .advanceWatermarkTo(new Instant(0L).plus(Duration.standardMinutes(2)))
            .addElements(
                getRow(schema, 2L, "1", new byte[] {2}, new byte[] {3}, "update"),
                getRow(schema, 2L, "2", new byte[] {0, 1, 2, 3}, null, "update"))
            .advanceWatermarkTo(new Instant(0L).plus(Duration.standardMinutes(3)))
            .addElements(
                getRow(schema, 3L, "1", null, null, "update"),
                getRow(schema, 3L, "2", null, null, "update"))
            .advanceWatermarkTo(new Instant(0L).plus(Duration.standardMinutes(4)))
            .addElements(getRow(schema, 4L, "1", null, null, "delete"))
            .advanceWatermarkToInfinity();

    var input = pipeline.apply(createInput).setCoder(RowCoder.of(schema));

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

    PAssert.that(output.get(Merge.LATEST))
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
