package com.hedera.etl.diff;

import lombok.RequiredArgsConstructor;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptors;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@RequiredArgsConstructor
public class Merge extends PTransform<PCollection<Row>, PCollectionTuple> {
  public static final TupleTag<Row> DIFFS = new TupleTag<>("diffs");
  public static final TupleTag<Row> UPDATED = new TupleTag<>("");
  public static final TupleTag<Row> LATEST = new TupleTag<>("latest");

  private final String idField;

  private final AbstractMerger mergerDoFn;

  public static Merge diffs(String idField, String timestampField) {
    return new Merge(idField, new DiffMerger(timestampField));
  }

  public static Merge sum(String idField, String timestampField, String summableField) {
    return new Merge(idField, new SumMerger(timestampField, summableField));
  }

  @Override
  public PCollectionTuple expand(PCollection<Row> input) {
    var inputCoder = input.getCoder();

    var groupedRows = input
            .apply("Key by %s".formatted(idField), WithKeys.of(row -> getValueFromRowOrNested(String.class, row,
                    idField)))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), inputCoder))
            .apply("Group by key", GroupByKey.create())
            .setCoder(KvCoder.of(StringUtf8Coder.of(), IterableCoder.of(inputCoder)));

    var mergedRows = groupedRows
            .apply("Remove keys", MapElements
                    .into(TypeDescriptors.iterables(TypeDescriptors.rows()))
                    .via(kv -> kv.getValue())).setCoder(IterableCoder.of(inputCoder))
            .apply("Merge diffs", ParDo
                    .of(mergerDoFn)
                    .withOutputTags(UPDATED, TupleTagList.of(LATEST)));

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
  public static abstract class AbstractMerger extends DoFn<Iterable<Row>, Row> {
    protected final String timestampField;

    @ProcessElement
    public void processElement(ProcessContext c) {
      var rows = c.element();

      var result = mergeDiffs(rows);
      for (var row : result) {
        c.output(row);
      }

      c.output(LATEST, result.getLast());
    }

    private List<Row> mergeDiffs(Iterable<Row> rows) {
      var sortedRows = StreamSupport.stream(rows.spliterator(), false)
              .sorted((row1, row2) -> Long.compare(
                      getValueFromRowOrNested(Long.class, row1, this.timestampField),
                      getValueFromRowOrNested(Long.class, row2, this.timestampField)
              ))
              .collect(Collectors.toList());

      var result = new ArrayList<Row>();

      var lastRow = sortedRows.removeFirst();
      result.add(lastRow);
      for (var row : sortedRows) {
        lastRow = updateRow(lastRow, row);
        result.add(lastRow);
      }

      return result;
    }

    protected abstract Row updateRow(Row lastRow, Row diffRow);
  }

  public static class DiffMerger extends AbstractMerger {
    public DiffMerger(String timestampField) {
      super(timestampField);
    }

    @Override
    protected Row updateRow(Row lastRow, Row diffRow) {
      var fields = diffRow.getSchema().getFieldNames();

      var rowBuilder = Row.fromRow(lastRow);

      for (var field : fields) {
        var newValue = diffRow.getValue(field);
        if (newValue != null) {
          rowBuilder = rowBuilder.withFieldValue(field, diffRow.getValue(field));
        }
      }

      return rowBuilder.build();
    }
  }

  public static class SumMerger extends AbstractMerger {

    private final String summableField;

    public SumMerger(String timestampField, String summableField) {
      super(timestampField);
      this.summableField = summableField;
    }

    @Override
    protected Row updateRow(Row lastRow, Row diffRow) {
      var lastValue = lastRow.getInt64(summableField);
      var newValue = diffRow.getInt64(summableField) + lastValue;

      return Row.fromRow(diffRow)
              .withFieldValue(summableField, newValue)
              .build();
    }
  }
}
