/*
 * Copyright (C) 2021-2024 Hedera Hashgraph, LLC
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

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;

import com.hedera.etl.reader.recordfile.domain.StreamFileSignature;
import com.hedera.etl.reader.recordfile.domain.StreamFilename;

@Log4j2
@RequiredArgsConstructor
public class CompositeSignatureFileReader implements SignatureFileReader {

  private final SignatureFileReaderV2 signatureFileReaderV2 = new SignatureFileReaderV2();
  private final SignatureFileReaderV5 signatureFileReaderV5 = new SignatureFileReaderV5();
  private final ProtoSignatureFileReader protoSignatureFileReader = new ProtoSignatureFileReader();

  @Override
  public StreamFileSignature read(StreamFilename filename, byte[] bytes) {
    try (DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(bytes))) {
      byte version = dataInputStream.readByte();
      SignatureFileReader fileReader;

      if (version == SignatureFileReaderV5.VERSION) {
        fileReader = signatureFileReaderV5;
      } else if (version
          <= SignatureFileReaderV2.SIGNATURE_TYPE_FILE_HASH) { // Begins with a byte of value 4
        fileReader = signatureFileReaderV2;
      } else if (version == ProtoSignatureFileReader.VERSION) {
        fileReader = protoSignatureFileReader;
      } else {
        throw new RuntimeException("Unsupported signature file version: " + version);
      }

      return fileReader.read(filename, bytes);
    } catch (IOException ex) {
      throw new RuntimeException("Error reading signature file", ex);
    }
  }
}
