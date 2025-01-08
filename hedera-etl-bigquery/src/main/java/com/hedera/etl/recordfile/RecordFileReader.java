package com.hedera.etl.recordfile;

import com.hedera.etl.recordfile.entity.RecordFile;

public interface RecordFileReader {
    int MAX_TRANSACTION_LENGTH = 64 * 1024;

    RecordFileReader INSTANCE = new CompositeRecordFileReader();

    RecordFile read(StreamFilename filename, byte[] bytes);
}

