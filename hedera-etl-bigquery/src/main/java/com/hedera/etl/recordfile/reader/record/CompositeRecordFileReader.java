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

package com.hedera.etl.recordfile.reader.record;

import com.google.common.base.Stopwatch;

import com.hedera.etl.recordfile.domain.StreamFilename;
import com.hedera.etl.recordfile.domain.transaction.RecordFile;

import com.hedera.etl.recordfile.exception.InvalidStreamFileException;

import com.hedera.etl.recordfile.exception.StreamFileReaderException;

import lombok.CustomLog;
import lombok.RequiredArgsConstructor;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

@CustomLog
@RequiredArgsConstructor
public class CompositeRecordFileReader implements RecordFileReader {

    private final RecordFileReaderImplV1 version1Reader = new RecordFileReaderImplV1();
    private final RecordFileReaderImplV2 version2Reader = new RecordFileReaderImplV2();
    private final RecordFileReaderImplV5 version5Reader = new RecordFileReaderImplV5();
    private final ProtoRecordFileReader version6Reader = new ProtoRecordFileReader();

    @Override
    public RecordFile read(StreamFilename filename, byte[] bytes) {
        long count = 0;
        Stopwatch stopwatch = Stopwatch.createStarted();
        int version = 0;

        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes))) {
            RecordFileReader reader;
            version = dis.readInt();

            switch (version) {
                case 1:
                    reader = version1Reader;
                    break;
                case 2:
                    reader = version2Reader;
                    break;
                case 5:
                    reader = version5Reader;
                    break;
                case 6:
                    reader = version6Reader;
                    break;
                default:
                    throw new InvalidStreamFileException(
                            String.format("Unsupported record file version %d in file %s", version, filename));
            }

            RecordFile recordFile = reader.read(filename, bytes);
            count = recordFile.getCount();
            return recordFile;
        } catch (IOException e) {
            throw new StreamFileReaderException("Error reading record file " + filename, e);
        } finally {
            log.debug(
                    "Read {} items {}successfully from v{} record file {} in {}",
                    count,
                    count != 0 ? "" : "un",
                    version,
                    filename,
                    stopwatch);
        }
    }
}
