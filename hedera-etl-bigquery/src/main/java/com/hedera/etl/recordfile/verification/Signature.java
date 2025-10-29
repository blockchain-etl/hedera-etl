package com.hedera.etl.recordfile.verification;

import java.io.Serializable;

import javax.annotation.Nullable;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

import com.hedera.etl.reader.recordfile.domain.StreamFileSignature;
import com.hedera.etl.reader.recordfile.domain.StreamFilename;
import com.hedera.etl.reader.recordfile.reader.signature.SignatureFileReader;

@DefaultSchema(JavaBeanSchema.class)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Signature implements Serializable {
  @Nullable private String filename;
  @Nullable private String fileHash;
  @Nullable private String metadataHash;

  public static Signature fromBytes(String filename, byte[] bytes) {
    return fromStreamFile(SignatureFileReader.INSTANCE.read(StreamFilename.from(filename), bytes));
  }

  public static Signature fromStreamFile(StreamFileSignature signature) {
    return builder()
        .filename(signature.getDataFilename().getFilename())
        .fileHash(signature.getFileHashAsHex())
        .metadataHash(signature.getMetadataHashAsHex())
        .build();
  }
}
