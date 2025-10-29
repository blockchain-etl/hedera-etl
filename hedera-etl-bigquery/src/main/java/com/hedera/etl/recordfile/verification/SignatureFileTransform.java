package com.hedera.etl.recordfile.verification;

import java.io.IOException;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.hedera.etl.reader.recordfile.domain.StreamFileSignature;
import com.hedera.etl.reader.recordfile.domain.StreamFilename;
import com.hedera.etl.reader.recordfile.reader.signature.SignatureFileReader;

@Slf4j
public class SignatureFileTransform
    extends PTransform<PCollection<FileIO.ReadableFile>, PCollection<Signature>> {

  @SneakyThrows
  @NonNull @Override
  public PCollection<Signature> expand(@NonNull PCollection<FileIO.ReadableFile> input) {
    return input
        .apply(
            "Read Signature Files",
            MapElements.into(TypeDescriptor.of(StreamFileSignature.class))
                .via(
                    file -> {
                      log.debug("Parsing file {}", file.getMetadata().resourceId());
                      try {
                        return SignatureFileReader.INSTANCE.read(
                            StreamFilename.from(file.getMetadata().resourceId().getFilename()),
                            file.readFullyAsBytes());
                      } catch (IOException e) {
                        throw new RuntimeException(e);
                      }
                    }))
        //        .apply(
        //            "Verify signatures",
        //            Filter.by(
        //                streamFileSignature -> {
        //                 //  we have no access to node from here
        //                  var publicKey =
        //                      streamFileSignature != null
        //                          ? streamFileSignature.getNode().getPublicKey()
        //                          : null;
        //
        //                  if (publicKey == null) {
        //                    log.warn("Missing PublicKey for node {}",
        // streamFileSignature.getNode());
        //                    return false;
        //                  }
        //
        //                  if (streamFileSignature.getFileHashSignature() == null) {
        //                    log.error("Missing signature data: {}", streamFileSignature);
        //                    return false;
        //                  }
        //
        //                  try {
        //                    log.trace("Verifying signature: {}", streamFileSignature);
        //
        //                    java.security.Signature sig =
        //                        java.security.Signature.getInstance(
        //                            streamFileSignature.getSignatureType().getAlgorithm(),
        //                            streamFileSignature.getSignatureType().getProvider());
        //                    sig.initVerify(publicKey);
        //                    sig.update(streamFileSignature.getFileHash());
        //
        //                    if (!sig.verify(streamFileSignature.getFileHashSignature())) {
        //                      return false;
        //                    }
        //
        //                    if (streamFileSignature.getMetadataHashSignature() != null) {
        //                      sig.update(streamFileSignature.getMetadataHash());
        //                      return sig.verify(streamFileSignature.getMetadataHashSignature());
        //                    }
        //
        //                    return true;
        //                  } catch (Exception e) {
        //                    log.error(
        //                        "Failed to verify signature with public key {}: {}",
        //                        publicKey,
        //                        streamFileSignature,
        //                        e);
        //                  }
        //                  return false;
        //                }))
        .apply(
            "Map to internal signatures",
            MapElements.into(TypeDescriptor.of(Signature.class))
                .via(
                    signatureFile ->
                        Signature.builder()
                            .filename(signatureFile.getDataFilename().getFilename())
                            .fileHash(signatureFile.getFileHashAsHex())
                            .metadataHash(signatureFile.getMetadataHashAsHex())
                            .build()));
  }
}
