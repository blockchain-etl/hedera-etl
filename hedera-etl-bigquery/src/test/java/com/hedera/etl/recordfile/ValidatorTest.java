package com.hedera.etl.recordfile;

import java.time.Duration;
import java.util.List;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Rule;
import org.junit.Test;

import com.hedera.etl.reader.recordfile.domain.transaction.RecordFile;

public class ValidatorTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testPreviousHashState() {
    var input =
        pipeline
            .apply(
                Create.timestamped(
                    List.of(
                        new SignedRecordFileStub("1970-01-01T00_00_00.000000000Z.rcd", "hash0", "---"),
                        new SignedRecordFileStub("1970-01-01T00_01_00.000000000Z.rcd", "hash1", "hash0"),
                        new SignedRecordFileStub("1970-01-01T00_02_00.000000000Z.rcd", "hash2", "hash1"),
                        new SignedRecordFileStub("1970-01-01T00_03_00.000000000Z.rcd", "hash3", "hash2")),
                    List.of(0L, 1L, 2L, 3L).stream()
                        .map(x -> Duration.ofMinutes(x).toMillis())
                        .toList()))
            .apply(WithKeys.of(0));

    var result =
        input
            .apply(ParDo.of(new Validator.ValidatorDoFn("---")))
            .apply("Global", Window.into(new GlobalWindows()))
            .apply(MapElements.into(TypeDescriptors.strings()).via(f -> f.getName()));

    PAssert.that(result).containsInAnyOrder("1970-01-01T00_00_00.000000000Z.rcd", "1970-01-01T00_01_00.000000000Z.rcd", "1970-01-01T00_02_00.000000000Z.rcd", "1970-01-01T00_03_00.000000000Z.rcd");

    pipeline.run();
  }

  @Test
  public void testPreviousHashStateWithDuplicates() {
    var input =
        pipeline
            .apply(
                Create.timestamped(
                    List.of(
                        new SignedRecordFileStub("1970-01-01T00_00_00.000000000Z.rcd", "hash0", "---"),
                        new SignedRecordFileStub("1970-01-01T00_01_00.000000000Z.rcd", "hash1", "hash0"),
                        new SignedRecordFileStub("1970-01-01T00_02_00.000000000Z.rcd", "hash2", "hash1"),
                        new SignedRecordFileStub("1970-01-01T00_02_00.000000000Z.rcd", "hash2", "hash1"),
                        new SignedRecordFileStub("1970-01-01T00_03_00.000000000Z.rcd", "hash3", "hash2")),
                    List.of(0L, 1L, 2L, 2L, 3L).stream()
                        .map(x -> Duration.ofMinutes(x).toMillis())
                        .toList()))
            .apply(WithKeys.of(0));

    var result =
        input
            .apply(ParDo.of(new Validator.ValidatorDoFn("---")))
            .apply("Global", Window.into(new GlobalWindows()))
            .apply(MapElements.into(TypeDescriptors.strings()).via(f -> f.getName()));

    PAssert.that(result).containsInAnyOrder("1970-01-01T00_00_00.000000000Z.rcd", "1970-01-01T00_01_00.000000000Z.rcd", "1970-01-01T00_02_00.000000000Z.rcd", "1970-01-01T00_03_00.000000000Z.rcd");

    pipeline.run();
  }

  @Test
  public void testPreviousHashStateWithDuplicatesWhereOneIsBad() {
    var input =
        pipeline
            .apply(
                Create.timestamped(
                    List.of(
                        new SignedRecordFileStub("1970-01-01T00_00_00.000000000Z.rcd", "hash0", "---"),
                        new SignedRecordFileStub("1970-01-01T00_01_00.000000000Z.rcd", "hash1", "hash0"),
                        new SignedRecordFileStub("1970-01-01T00_02_00.000000000Z.rcd", "bad-hash2", "bad-hash1"),
                        new SignedRecordFileStub("1970-01-01T00_02_00.000000000Z.rcd", "bad-hash2", "bad-hash1"),
                        new SignedRecordFileStub("1970-01-01T00_02_00.000000000Z.rcd", "bad-hash2", "bad-hash1"),
                        new SignedRecordFileStub("1970-01-01T00_02_00.000000000Z.rcd", "bad-hash2", "bad-hash1"),
                        new SignedRecordFileStub("1970-01-01T00_02_00.000000000Z.rcd", "hash2", "hash1"),
                        new SignedRecordFileStub("1970-01-01T00_03_00.000000000Z.rcd", "hash3", "hash2")),
                    List.of(0L, 1L, 2L, 2L, 2L, 2L, 2L, 3L).stream()
                        .map(x -> Duration.ofMinutes(x).toMillis())
                        .toList()))
            .apply(WithKeys.of(0));

    var result =
        input
            .apply(ParDo.of(new Validator.ValidatorDoFn("---")))
            .apply("Global", Window.into(new GlobalWindows()))
            .apply(MapElements.into(TypeDescriptors.strings()).via(f -> f.getName()));

    PAssert.that(result).containsInAnyOrder("1970-01-01T00_00_00.000000000Z.rcd", "1970-01-01T00_01_00.000000000Z.rcd", "1970-01-01T00_02_00.000000000Z.rcd", "1970-01-01T00_03_00.000000000Z.rcd");

    pipeline.run();
  }

  private static class SignedRecordFileStub extends SignedRecordFilesHandler {
    public SignedRecordFileStub(String filename, String hash, String previousHash) {
      super(
          filename,
          List.of(
              RecordFile.builder().name(filename).hash(hash).previousHash(previousHash).build()));
    }
  }
}
