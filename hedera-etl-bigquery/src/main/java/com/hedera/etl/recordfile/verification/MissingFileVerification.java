package com.hedera.etl.recordfile.verification;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Objects;
import java.util.stream.StreamSupport;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.io.gcp.bigquery.CombineAsIterable;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Latest;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

import com.hedera.etl.reader.recordfile.domain.transaction.RecordFile;

@Slf4j
public class MissingFileVerification
    extends PTransform<PCollection<RecordFile>, PCollection<RecordFile>> {
  @Override
  public PCollection<RecordFile> expand(PCollection<RecordFile> input) {
    var hashes =
        input
            .apply(
                "Map to hashes",
                MapElements.into(TypeDescriptor.of(Hashes.class))
                    .via(
                        file ->
                            Hashes.builder()
                                .filename(file.getName())
                                .currentHash(file.getHash())
                                .previousHash(file.getPreviousHash())
                                .build()))
            .apply("Group hashes", new CombineAsIterable<>())
            .apply(
                "Filtered hashes",
                MapElements.into(TypeDescriptors.sets(TypeDescriptors.strings()))
                    .via(
                        hs -> {
                          var orderedHashes =
                              StreamSupport.stream(hs.spliterator(), false)
                                  .sorted(Comparator.comparing(h -> h.filename))
                                  .toList();

                          var result = new HashSet<String>();

                          orderedHashes.stream().findFirst().ifPresent(h -> result.add(h.filename));

                          for (int i = 1; i < orderedHashes.size(); i++) {
                            var prev = orderedHashes.get(i - 1);
                            var next = orderedHashes.get(i);
                            if (!Objects.equals(next.previousHash, prev.currentHash)) {
                              log.error(
                                  "Hashes between files {} ({}), and {} ({}) don't match",
                                  prev.filename,
                                  prev.currentHash,
                                  next.filename,
                                  next.previousHash);
                              log.error("Stopping processing files beyond {}", prev.filename);
                              break;
                            }
                            result.add(next.filename);
                          }

                          return result;
                        }))
            .apply("Get value", Latest.globally())
            .apply("As View", View.asSingleton());

    return input.apply(
        "Filter hashes",
        ParDo.of(
                new DoFn<RecordFile, RecordFile>() {
                  @ProcessElement
                  public void process(ProcessContext c) {
                    var file = c.element();
                    var validFileSet = c.sideInput(hashes);

                    if (validFileSet.contains(file.getName())) {
                      c.output(file);
                    } else {
                      log.warn("Ignoring file {}", file.getName());
                    }
                  }
                })
            .withSideInputs(hashes));
  }

  @DefaultSchema(JavaBeanSchema.class)
  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  @Builder
  public static class Hashes {
    private String filename;
    private String currentHash;
    private String previousHash;
  }
}
