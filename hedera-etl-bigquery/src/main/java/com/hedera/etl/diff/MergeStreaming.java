package com.hedera.etl.diff;

import java.io.Serializable;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.collect.Streams;
import com.google.common.primitives.Bytes;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Latest;
import org.apache.beam.sdk.transforms.MapValues;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reify;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;

import com.hedera.etl.util.FormatUtils;

import static com.hedera.etl.diff.Merge.DIFFS;
import static com.hedera.etl.diff.Merge.LATEST;
import static com.hedera.etl.diff.Merge.UPDATED;

@RequiredArgsConstructor
public class MergeStreaming extends PTransform<PCollection<Row>, PCollectionTuple> {

  private static final org.joda.time.Duration PREAGGREGATE_WINDOW_DURATION =
      org.joda.time.Duration.standardSeconds(60);

  private final String idField;

  private final SerializableFunction<Schema, AbstractMergerDoFn> mergerDoFnProvider;

  private final RowCombiner combiner;

  public static MergeStreaming diffs(String idField, String timestampField) {
    return new MergeStreaming(idField, schema -> new DiffMerger(timestampField, schema), null);
  }

  public static MergeStreaming sum(String idField, String timestampField, String summableField) {
    return new MergeStreaming(
        idField,
        schema -> new SumMerger(timestampField, summableField, schema),
        new SumRowCombiner(summableField));
  }

  public static MergeStreaming append(
      String idField, String timestampField, String appendableField, String actionField) {
    return new MergeStreaming(
        idField,
        schema -> new AppendMerger(timestampField, appendableField, actionField, schema),
        null);
  }

  @Override
  public PCollectionTuple expand(PCollection<Row> input) {
    var inputCoder = input.getCoder();

    final var inputSchema = input.getSchema();
    final var longFields =
        inputSchema.getFields().stream()
            .map(Schema.Field::getName)
            .filter(
                field ->
                    Set.of(
                            "consensus_timestamp",
                            "created_timestamp",
                            "modified_timestamp",
                            "timestamp")
                        .contains(field))
            .collect(Collectors.toSet());
    final var timestampFields =
        inputSchema.getFields().stream()
            .map(Schema.Field::getName)
            .filter(field -> Set.of("created", "modified").contains(field))
            .collect(Collectors.toSet());

    var windowedRows =
        combiner == null
            ? input
            : input.apply(
                "Add preaggregate window",
                Window.<Row>into(FixedWindows.of(PREAGGREGATE_WINDOW_DURATION))
                    .discardingFiredPanes()
                    .withTimestampCombiner(TimestampCombiner.END_OF_WINDOW));

    var keyedRows =
        windowedRows
            .apply(
                "Key by %s".formatted(idField),
                WithKeys.of(row -> getValueFromRowOrNested(String.class, row, idField)))
            .setCoder(
                KvCoder.of(NullableCoder.of(StringUtf8Coder.of()), NullableCoder.of(inputCoder)));

    var preaggregatedRows =
        combiner == null
            ? keyedRows
            : keyedRows
                .apply("Preaggregate", Combine.perKey(combiner::updateRow))
                .apply("Filter out nulls", Filter.by(kv -> kv.getValue() != null))
                .apply("Add timestamps", Reify.timestampsInValue())
                .apply(
                    "Update timestamps",
                    MapValues.into(TypeDescriptors.rows())
                        .via(
                            tv -> {
                              var timestampInNanos = tv.getTimestamp().getMillis() * 1_000_000L;
                              var timestamp = FormatUtils.timestampFromNanos(timestampInNanos);
                              var rowBuilder = Row.fromRow(tv.getValue());
                              longFields.forEach(
                                  field -> rowBuilder.withFieldValue(field, timestampInNanos));
                              timestampFields.forEach(
                                  field -> rowBuilder.withFieldValue(field, timestamp));

                              return rowBuilder.build();
                            }))
                .setCoder(
                    KvCoder.of(
                        NullableCoder.of(StringUtf8Coder.of()), NullableCoder.of(inputCoder)))
                .apply("Reify preaggregate window to global", Window.into(new GlobalWindows()));

    var mergedRows =
        preaggregatedRows
            .apply(
                "Add grouping window",
                Window.<KV<String, Row>>into(FixedWindows.of(PREAGGREGATE_WINDOW_DURATION))
                    .discardingFiredPanes()
                    .withTimestampCombiner(TimestampCombiner.END_OF_WINDOW))
            .apply("Group by key", GroupByKey.create())
            .setCoder(
                KvCoder.of(
                    (NullableCoder.of(StringUtf8Coder.of())),
                    IterableCoder.of(NullableCoder.of(inputCoder))))
            .apply("Reify window to global", Window.into(new GlobalWindows()))
            .apply("Merge diffs", ParDo.of(mergerDoFnProvider.apply(inputSchema)));

    var latest =
        mergedRows
            .setCoder(inputCoder)
            .apply(
                "Group into daily window",
                Window.<Row>into(FixedWindows.of(Duration.standardDays(1)))
                    .discardingFiredPanes()
                    .withTimestampCombiner(TimestampCombiner.END_OF_WINDOW))
            .setCoder(NullableCoder.of(inputCoder))
            .apply(
                "Key latest by %s".formatted(idField),
                WithKeys.of(row -> getValueFromRowOrNested(String.class, row, idField)))
            .setCoder(
                KvCoder.of(NullableCoder.of(StringUtf8Coder.of()), NullableCoder.of(inputCoder)))
            .apply("Find latest", Latest.perKey())
            .apply("Drop key", Values.create())
            .apply("Reify daily window to global", Window.into(new GlobalWindows()))
            .setCoder(inputCoder);

    return PCollectionTuple.empty(mergedRows.getPipeline())
        .and(DIFFS, input)
        .and(UPDATED, mergedRows)
        .and(LATEST, latest);
  }

  private static <T> T getValueFromRowOrNested(Class<T> type, Row row, String field) {
    var fields = field.split("\\.");
    var current = row;
    for (int i = 0; i < fields.length - 1; i++) {
      current = row.getRow(fields[i]);
    }

    return current.getValue(fields[fields.length - 1]);
  }

  @Log4j2
  public abstract static class AbstractMergerDoFn extends DoFn<KV<String, Iterable<Row>>, Row> {

    protected final String timestampField;
    protected final RowCombiner combiner;
    protected final Schema schema;

    @StateId("lastRow")
    private final StateSpec<ValueState<Row>> lastRow;

    @StateId("lastTs")
    private final StateSpec<ValueState<Instant>> lastTs;

    protected AbstractMergerDoFn(String timestampField, RowCombiner combiner, Schema schema) {
      this.timestampField = timestampField;
      this.combiner = combiner;
      this.schema = schema;
      this.lastRow = StateSpecs.rowValue(schema);
      this.lastTs = StateSpecs.value();
    }

    @ProcessElement
    public void processElement(ProcessContext c, @StateId("lastRow") ValueState<Row> lastRowState) {

      var kv = c.element();
      if (kv == null || kv.getKey() == null || kv.getValue() == null) {
        return;
      }
      var rows = kv.getValue();

      var lastRowFromState = lastRowState.read();

      var sortedRowsIterator =
          Streams.concat(
                  Stream.of(lastRowFromState), StreamSupport.stream(rows.spliterator(), false))
              .filter(Objects::nonNull)
              .sorted(
                  (row1, row2) -> {
                    var ts1 = getValueFromRowOrNested(Long.class, row1, this.timestampField);
                    var ts2 = getValueFromRowOrNested(Long.class, row2, this.timestampField);
                    return Long.compare(ts1 != null ? ts1 : -1, ts2 != null ? ts2 : -1);
                  })
              .iterator();

      var lastRow = sortedRowsIterator.next();
      outputIfNotEqualTo(c, lastRowFromState, lastRow);

      while (sortedRowsIterator.hasNext()) {
        lastRow = combiner.updateRow(lastRow, sortedRowsIterator.next());
        outputIfNotEqualTo(c, lastRowFromState, lastRow);
      }
      lastRowState.write(lastRow);
    }

    private void outputIfNotEqualTo(DoFn.ProcessContext context, Row test, Row output) {
      if (!Objects.equals(test, output)) {
        context.output(output);
      }
    }
  }

  public static class DiffMerger extends AbstractMergerDoFn {
    public DiffMerger(String timestampField, Schema schema) {
      super(timestampField, new RowCombiner(), schema);
    }
  }

  public static class SumMerger extends AbstractMergerDoFn {
    public SumMerger(String timestampField, String summableField, Schema schema) {
      super(timestampField, new SumRowCombiner(summableField), schema);
    }
  }

  public static class AppendMerger extends AbstractMergerDoFn {
    public AppendMerger(
        String timestampField, String appendableField, String actionField, Schema schema) {
      super(timestampField, new AppendRowCombiner(appendableField, actionField), schema);
    }
  }

  public static class RowCombiner implements Serializable {
    protected Row updateRow(Row lastRow, Row diffRow) {
      var fields = diffRow.getSchema().getFieldNames();

      var rowBuilder = Row.fromRow(lastRow);

      for (var field : fields) {
        var newValue = diffRow.getValue(field);
        if (newValue != null) {
          if (!(newValue instanceof String) || !((String) newValue).isBlank()) {
            rowBuilder = rowBuilder.withFieldValue(field, newValue);
          }
        }
      }

      return rowBuilder.build();
    }
  }

  @RequiredArgsConstructor
  public static class SumRowCombiner extends RowCombiner {
    private final String summableField;

    @Override
    protected Row updateRow(Row lastRow, Row diffRow) {
      var row = super.updateRow(lastRow, diffRow);

      var lastValue = lastRow.getDecimal(summableField);
      var newValue = diffRow.getDecimal(summableField).add(lastValue);

      return Row.fromRow(row).withFieldValue(summableField, newValue).build();
    }
  }

  @RequiredArgsConstructor
  public static class AppendRowCombiner extends RowCombiner {
    private final String appendableField;
    private final String actionField;

    @Override
    protected Row updateRow(Row lastRow, Row diffRow) {
      var row = super.updateRow(lastRow, diffRow);

      var lastValue = lastRow.getBytes(appendableField);
      var currentValue = diffRow.getBytes(appendableField);

      byte[] newValue = null;

      if (lastValue == null) {
        newValue = currentValue;
      } else if (currentValue == null) {
        if (!Objects.equals(diffRow.getString(actionField), "delete")) {
          newValue = lastValue;
        }
      } else {
        if (Objects.equals(diffRow.getString(actionField), "update")) {
          newValue = Bytes.concat(lastValue, currentValue);
        } else if (Objects.equals(diffRow.getString(actionField), "create")) {
          newValue = currentValue;
        }
      }

      return Row.fromRow(row).withFieldValue(appendableField, newValue).build();
    }
  }
}
