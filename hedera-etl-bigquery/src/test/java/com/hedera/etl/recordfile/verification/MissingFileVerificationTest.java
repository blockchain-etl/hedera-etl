package com.hedera.etl.recordfile.verification;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.junit.Rule;
import org.junit.Test;

import com.hedera.etl.reader.recordfile.domain.transaction.RecordFile;

public class MissingFileVerificationTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private final RecordFile firstRecordFile =
      RecordFile.builder().name("filename1.rcd").hash("hash1").previousHash("hash0").build();

  private final RecordFile secondRecordFile =
      RecordFile.builder().name("filename2.rcd").hash("hash2").previousHash("hash1").build();

  private final RecordFile thirdRecordFile =
      RecordFile.builder().name("filename3.rcd").hash("hash3").previousHash("hash2").build();

  private final RecordFile forthRecordFile =
      RecordFile.builder().name("filename4.rcd").hash("hash4").previousHash("hash3").build();

  @Test
  public void testVerificationWithoutMissingFiles() {
    // given
    var input =
        pipeline.apply(
            Create.of(firstRecordFile, secondRecordFile, thirdRecordFile, forthRecordFile));

    // when
    var output = input.apply(new MissingFileVerification());

    // then
    PAssert.that(output)
        .containsInAnyOrder(firstRecordFile, secondRecordFile, thirdRecordFile, forthRecordFile);

    pipeline.run();
  }

  @Test
  public void testVerificationWithMissingFile() {
    // given
    var input = pipeline.apply(Create.of(firstRecordFile, secondRecordFile, forthRecordFile));

    // when
    var output = input.apply(new MissingFileVerification());

    // then
    PAssert.that(output).containsInAnyOrder(firstRecordFile, secondRecordFile);

    pipeline.run();
  }
}
