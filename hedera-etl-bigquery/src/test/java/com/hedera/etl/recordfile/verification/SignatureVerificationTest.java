package com.hedera.etl.recordfile.verification;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.junit.Rule;
import org.junit.Test;

import com.hedera.etl.reader.recordfile.domain.transaction.RecordFile;

public class SignatureVerificationTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private final RecordFile firstRecordFile =
      RecordFile.builder()
          .name("filename1.rcd")
          .fileHash("file_hash1")
          .metadataHash("metadata_hash1")
          .build();

  private final Signature firstSignature =
      Signature.builder()
          .filename("filename1.rcd")
          .fileHash("file_hash1")
          .metadataHash("metadata_hash1")
          .build();

  private final RecordFile secondRecordFile =
      RecordFile.builder()
          .name("filename2.rcd")
          .fileHash("file_hash2")
          .metadataHash("metadata_hash2")
          .build();

  private final Signature secondSignature =
      Signature.builder()
          .filename("filename2.rcd")
          .fileHash("file_hash2")
          .metadataHash("metadata_hash2")
          .build();

  private final RecordFile thirdRecordFile =
      RecordFile.builder()
          .name("filename3.rcd")
          .fileHash("file_hash3")
          .metadataHash("metadata_hash3")
          .build();

  private final Signature thirdSignature =
      Signature.builder()
          .filename("filename3.rcd")
          .fileHash("file_hash3")
          .metadataHash("metadata_hash3")
          .build();

  private final RecordFile fourthRecordFile =
      RecordFile.builder()
          .name("filename4.rcd")
          .fileHash("file_hash4")
          .metadataHash("metadata_hash4")
          .build();

  private final Signature fourthSignature =
      Signature.builder()
          .filename("filename4.rcd")
          .fileHash("file_hash4")
          .metadataHash("metadata_hash4")
          .build();

  @Test
  public void testVerificationWithoutMismatches() {
    // given
    var recordFiles =
        pipeline.apply(
            "Create record files",
            Create.of(firstRecordFile, secondRecordFile, thirdRecordFile, fourthRecordFile));
    var signatures =
        pipeline.apply(
            "Create signatures",
            Create.of(firstSignature, secondSignature, thirdSignature, fourthSignature));

    // when
    var output = SignatureVerification.verifyWithSignatures(recordFiles, signatures);

    // then
    PAssert.that(output)
        .containsInAnyOrder(firstRecordFile, secondRecordFile, thirdRecordFile, fourthRecordFile);

    pipeline.run();
  }

  @Test
  public void testVerificationWithMismatchedFileHash() {
    // given
    var mismatchedThirdSignature =
        Signature.builder()
            .filename(thirdSignature.getFilename())
            .fileHash(thirdSignature.getFileHash() + "_INVALID")
            .metadataHash(thirdSignature.getMetadataHash())
            .build();

    var recordFiles =
        pipeline.apply(
            "Create record files",
            Create.of(firstRecordFile, secondRecordFile, thirdRecordFile, fourthRecordFile));
    var signatures =
        pipeline.apply(
            "Create signatures",
            Create.of(firstSignature, secondSignature, mismatchedThirdSignature, fourthSignature));

    // when
    var output = SignatureVerification.verifyWithSignatures(recordFiles, signatures);

    // then
    PAssert.that(output).containsInAnyOrder(firstRecordFile, secondRecordFile, fourthRecordFile);

    pipeline.run();
  }

  @Test
  public void testVerificationWithMismatchedMetadataHash() {
    // given
    var mismatchedThirdSignature =
        Signature.builder()
            .filename(thirdSignature.getFilename())
            .fileHash(thirdSignature.getFileHash())
            .metadataHash(thirdSignature.getMetadataHash() + "_INVALID")
            .build();

    var recordFiles =
        pipeline.apply(
            "Create record files",
            Create.of(firstRecordFile, secondRecordFile, thirdRecordFile, fourthRecordFile));
    var signatures =
        pipeline.apply(
            "Create signatures",
            Create.of(firstSignature, secondSignature, mismatchedThirdSignature, fourthSignature));

    // when
    var output = SignatureVerification.verifyWithSignatures(recordFiles, signatures);

    // then
    PAssert.that(output).containsInAnyOrder(firstRecordFile, secondRecordFile, fourthRecordFile);

    pipeline.run();
  }

  @Test
  public void testVerificationWithMissingSignature() {
    // given

    var recordFiles =
        pipeline.apply(
            "Create record files",
            Create.of(firstRecordFile, secondRecordFile, thirdRecordFile, fourthRecordFile));
    var signatures =
        pipeline.apply(
            "Create signatures", Create.of(firstSignature, secondSignature, fourthSignature));

    // when
    var output = SignatureVerification.verifyWithSignatures(recordFiles, signatures);

    // then
    PAssert.that(output).containsInAnyOrder(firstRecordFile, secondRecordFile, fourthRecordFile);

    pipeline.run();
  }
}
