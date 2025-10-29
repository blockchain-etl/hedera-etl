package com.hedera.etl.diff;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.google.common.primitives.Bytes;
import lombok.RequiredArgsConstructor;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.MapValues;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reify;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptors;

import com.hedera.etl.util.FormatUtils;

import static com.hedera.etl.diff.Merge.DIFFS;
import static com.hedera.etl.diff.Merge.LATEST;
import static com.hedera.etl.diff.Merge.UPDATED;

@RequiredArgsConstructor
public class MergeBatch extends PTransform<PCollection<Row>, PCollectionTuple> {

  private static final org.joda.time.Duration PREAGGREGATE_WINDOW_DURATION =
      org.joda.time.Duration.standardSeconds(60);

  private final List<String> idFields;

  private final AbstractMergerDoFn mergerDoFn;

  private final RowCombiner combiner;

  public static MergeBatch diffs(String idField, String timestampField) {
    return new MergeBatch(List.of(idField), new DiffMerger(timestampField), null);
  }

  public static MergeBatch sum(String idField, String timestampField, String summableField) {
    return new MergeBatch(
        List.of(idField),
        new SumMerger(timestampField, summableField),
        new SumRowCombiner(summableField));
  }

  public static MergeBatch sum(List<String> idFields, String timestampField, String summableField) {
    return new MergeBatch(
        idFields, new SumMerger(timestampField, summableField), new SumRowCombiner(summableField));
  }

  public static MergeBatch append(
      String idField, String timestampField, String appendableField, String actionField) {
    return new MergeBatch(
        List.of(idField), new AppendMerger(timestampField, appendableField, actionField), null);
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
                    .withTimestampCombiner(TimestampCombiner.END_OF_WINDOW));

    var keyedRows =
        windowedRows
            .apply(
                "Key by %s".formatted(String.join(", ", idFields)),
                WithKeys.of(
                    (Row row) ->
                        idFields.stream()
                            .map(idField -> getValueFromRowOrNested(String.class, row, idField))
                            .toList()))
            .setCoder(KvCoder.of(ListCoder.of(NullableCoder.of(StringUtf8Coder.of())), inputCoder));

    var preaggregatedRows =
        combiner == null
            ? keyedRows
            : keyedRows
                .apply("Preaggregate", Combine.perKey(combiner::updateRow))
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
                    KvCoder.of(ListCoder.of(NullableCoder.of(StringUtf8Coder.of())), inputCoder))
                .apply("Reify window to global", Window.into(new GlobalWindows()));

    var mergedRows =
        preaggregatedRows
            .apply("Group by key", GroupByKey.create())
            .setCoder(
                KvCoder.of(
                    ListCoder.of(NullableCoder.of(StringUtf8Coder.of())),
                    IterableCoder.of(inputCoder)))
            .apply(
                "Remove keys",
                MapElements.into(TypeDescriptors.iterables(TypeDescriptors.rows()))
                    .via(kv -> kv.getValue()))
            .setCoder(IterableCoder.of(inputCoder))
            .apply(
                "Merge diffs",
                ParDo.of(mergerDoFn).withOutputTags(UPDATED, TupleTagList.of(LATEST)));

    return PCollectionTuple.empty(mergedRows.getPipeline())
        .and(DIFFS, input)
        .and(UPDATED, mergedRows.get(UPDATED).setCoder(inputCoder))
        .and(LATEST, mergedRows.get(LATEST).setCoder(inputCoder));
  }

  private static <T> T getValueFromRowOrNested(Class<T> type, Row row, String field) {
    var fields = field.split("\\.");
    var current = row;
    for (int i = 0; i < fields.length - 1; i++) {
      current = row.getRow(fields[i]);
    }

    return current.getValue(fields[fields.length - 1]);
  }

  @RequiredArgsConstructor
  public abstract static class AbstractMergerDoFn extends DoFn<Iterable<Row>, Row> {
    protected final String timestampField;
    protected final RowCombiner combiner;

    @ProcessElement
    public void processElement(ProcessContext c) {
      var rows = c.element();

      var sortedRowsIterator =
          StreamSupport.stream(rows.spliterator(), false)
              .sorted(
                  (row1, row2) -> {
                    var ts1 = getValueFromRowOrNested(Long.class, row1, this.timestampField);
                    var ts2 = getValueFromRowOrNested(Long.class, row2, this.timestampField);
                    return Long.compare(ts1 != null ? ts1 : -1, ts2 != null ? ts2 : -1);
                  })
              .iterator();

      var lastRow = sortedRowsIterator.next();
      c.output(lastRow);

      while (sortedRowsIterator.hasNext()) {
        lastRow = combiner.updateRow(lastRow, sortedRowsIterator.next());
        c.output(lastRow);
      }

      emitLatest(c, lastRow);
    }

    protected void emitLatest(ProcessContext c, Row row) {
      c.output(LATEST, row);
    }
  }

  public static class DiffMerger extends AbstractMergerDoFn {
    public DiffMerger(String timestampField) {
      super(timestampField, new RowCombiner());
    }
  }

  public static class SumMerger extends AbstractMergerDoFn {
    private final String summableField;

    public SumMerger(String timestampField, String summableField) {
      super(timestampField, new SumRowCombiner(summableField));
      this.summableField = summableField;
    }

    @Override
    protected void emitLatest(DoFn<Iterable<Row>, Row>.ProcessContext c, Row row) {
      if (!Objects.equals(BigDecimal.ZERO, row.getDecimal(summableField))) {
        super.emitLatest(c, row);
      }
    }
  }

  public static class AppendMerger extends AbstractMergerDoFn {
    public AppendMerger(String timestampField, String appendableField, String actionField) {
      super(timestampField, new AppendRowCombiner(appendableField, actionField));
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
