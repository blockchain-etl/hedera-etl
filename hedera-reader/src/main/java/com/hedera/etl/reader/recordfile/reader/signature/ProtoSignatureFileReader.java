/*
 * Copyright (C) 2022-2024 Hedera Hashgraph, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hedera.etl.reader.recordfile.reader.signature;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import com.hedera.services.stream.proto.SignatureFile;

import com.hedera.etl.reader.recordfile.domain.StreamFileSignature;
import com.hedera.etl.reader.recordfile.domain.StreamFilename;
import com.hedera.etl.reader.recordfile.exception.InvalidStreamFileException;
import com.hedera.etl.reader.recordfile.utils.DomainUtils;

import static java.lang.String.format;

public class ProtoSignatureFileReader implements SignatureFileReader {

  public static final byte VERSION = 6;

  @Override
  public StreamFileSignature read(StreamFilename filename, byte[] bytes) {
    try {
      var signatureFile = readSignatureFile(filename, bytes);

      var fileSignature = signatureFile.getFileSignature();
      var metadataSignature = signatureFile.getMetadataSignature();

      var streamFileSignature = new StreamFileSignature();
      streamFileSignature.setBytes(bytes);
      streamFileSignature.setFileHash(DomainUtils.getHashBytes(fileSignature.getHashObject()));
      streamFileSignature.setFileHashSignature(DomainUtils.toBytes(fileSignature.getSignature()));
      streamFileSignature.setFilename(filename);
      streamFileSignature.setMetadataHash(
          DomainUtils.getHashBytes(metadataSignature.getHashObject()));
      streamFileSignature.setMetadataHashSignature(
          DomainUtils.toBytes(metadataSignature.getSignature()));
      streamFileSignature.setSignatureType(
          StreamFileSignature.SignatureType.valueOf(fileSignature.getType().toString()));
      streamFileSignature.setVersion(VERSION);

      return streamFileSignature;
    } catch (IllegalArgumentException | IOException e) {
      throw new InvalidStreamFileException(filename.getFilename(), e);
    }
  }

  private SignatureFile readSignatureFile(StreamFilename filename, byte[] bytes)
      throws IOException {
    try (var dataInputStream = new DataInputStream(new ByteArrayInputStream(bytes))) {
      byte version = dataInputStream.readByte();
      if (version != VERSION) {
        var message =
            format("Expected file %s with version %d, got %d", filename, VERSION, version);
        throw new InvalidStreamFileException(message);
      }

      var signatureFile = SignatureFile.parseFrom(dataInputStream);
      if (!signatureFile.hasFileSignature()) {
        throw new InvalidStreamFileException(
            format("The file %s does not have a file signature", filename));
      }

      if (!signatureFile.hasMetadataSignature()) {
        var message = format("The file %s does not have a file metadata signature", filename);
        throw new InvalidStreamFileException(message);
      }

      return signatureFile;
    }
  }
}
