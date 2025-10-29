package com.hedera.etl.recordfile.verification;

import java.util.Objects;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.hedera.etl.reader.recordfile.domain.transaction.RecordFile;

@Slf4j
public class SignatureVerification extends PTransform<PCollectionTuple, PCollection<RecordFile>> {

  private static TupleTag<RecordFile> RECORD_FILE = new TupleTag<>("record_File");
  private static TupleTag<Signature> SIGNATURE = new TupleTag<>("signature");

  public static PCollection<RecordFile> verifyWithSignatures(
      PCollection<RecordFile> recordFiles, PCollection<Signature> signatures) {
    return PCollectionTuple.of(RECORD_FILE, recordFiles)
        .and(SIGNATURE, signatures)
        .apply("Signature verification", new SignatureVerification());
  }

  @SneakyThrows
  @NonNull @Override
  public PCollection<RecordFile> expand(@NonNull PCollectionTuple input) {
    var signatures =
        input
            .get(SIGNATURE)
            .apply("Key by filename", WithKeys.of(sig -> sig.getFilename()))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(Signature.class)))
            .apply("As View", View.asMap());

    return input
        .get(RECORD_FILE)
        .apply(
            "Filter mismatched hashes",
            ParDo.of(
                    new DoFn<RecordFile, RecordFile>() {
                      @ProcessElement
                      public void process(ProcessContext c) {
                        var file = c.element();
                        var signaturesMap = c.sideInput(signatures);

                        if (!signaturesMap.containsKey(file.getName())) {
                          return;
                        }

                        var signature = signaturesMap.get(file.getName());

                        if (!Objects.equals(file.getFileHash(), signature.getFileHash())) {
                          log.warn(
                              "Mismatch in file {} file hash between {} and {}",
                              file.getName(),
                              file.getFileHash(),
                              signature.getFileHash());
                          return;
                        }

                        if (!Objects.equals(file.getMetadataHash(), signature.getMetadataHash())) {
                          log.warn(
                              "Mismatch in file {} metadata hash between {} and {}",
                              file.getName(),
                              file.getMetadataHash(),
                              signature.getMetadataHash());
                          return;
                        }

                        c.output(file);
                      }
                    })
                .withSideInputs(signatures));
  }
}
