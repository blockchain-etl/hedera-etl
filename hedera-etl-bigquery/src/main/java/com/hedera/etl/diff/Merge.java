package com.hedera.etl.diff;

import java.util.List;
import java.util.Optional;

import lombok.RequiredArgsConstructor;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.SerializableSupplier;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;

import com.hedera.etl.HederaETLApplication;

public class Merge {
  public static final TupleTag<Row> DIFFS = new TupleTag<>("diffs");
  public static final TupleTag<Row> UPDATED = new TupleTag<>("updated");
  public static final TupleTag<Row> LATEST = new TupleTag<>("latest");

  public static PTransform<PCollection<Row>, PCollectionTuple> diffs(
      String entityName, String idField, String timestampField) {
    return new ModeDependent(
        () -> MergeBatchWithHistory.diffs(entityName, idField, timestampField),
        () -> MergeStreamingWithHistory.diffs(entityName, idField, timestampField));
  }

  public static PTransform<PCollection<Row>, PCollectionTuple> sum(
      String entityName, String idField, String timestampField, String summableField) {
    return new ModeDependent(
        () -> MergeBatchWithHistory.sum(entityName, idField, timestampField, summableField),
        // Sum in streaming is disabled due to errors in calculating balances
        // () -> MergeStreamingWithHistory.sum(entityName, idField, timestampField, summableField));
        null);
  }

  public static PTransform<PCollection<Row>, PCollectionTuple> sum(
      String entityName, List<String> idFields, String timestampField, String summableField) {
    return new ModeDependent(
        () -> MergeBatchWithHistory.sum(entityName, idFields, timestampField, summableField),
        // Sum in streaming is disabled due to errors in calculating balances
        // () -> MergeStreamingWithHistory.sum(entityName, idField, timestampField, summableField));
        null);
  }

  public static PTransform<PCollection<Row>, PCollectionTuple> append(
      String entityName,
      String idField,
      String timestampField,
      String appendableField,
      String actionField) {
    return new ModeDependent(
        () ->
            MergeBatchWithHistory.append(
                entityName, idField, timestampField, appendableField, actionField),
        () ->
            MergeStreamingWithHistory.append(
                entityName, idField, timestampField, appendableField, actionField));
  }

  @RequiredArgsConstructor
  public static class ModeDependent extends PTransform<PCollection<Row>, PCollectionTuple> {
    private final SerializableSupplier<PTransform<PCollection<Row>, PCollectionTuple>> batch;
    private final SerializableSupplier<PTransform<PCollection<Row>, PCollectionTuple>> streaming;

    @Override
    public PCollectionTuple expand(PCollection<Row> input) {
      var mode =
          Optional.ofNullable(input.getPipeline().getOptions())
              .map(opts -> opts.as(HederaETLApplication.ApplicationOptions.class).getMode())
              .orElse(HederaETLApplication.ApplicationOptions.Mode.BATCH);

      return switch (mode) {
        case BATCH -> {
          if (batch != null) {
            yield input.apply(batch.get());
          } else {
            yield PCollectionTuple.of(DIFFS, input);
          }
        }
        case STREAMING -> {
          if (streaming != null) {
            yield input.apply(streaming.get());
          } else {
            yield PCollectionTuple.of(DIFFS, input);
          }
        }
      };
    }
  }
}
