package com.hedera.etl.recordfile.reader.record;

import com.hedera.etl.recordfile.domain.StreamFilename;
import com.hedera.etl.recordfile.domain.transaction.RecordFile;

public interface RecordFileReader {
    int MAX_TRANSACTION_LENGTH = 64 * 1024;

    RecordFileReader INSTANCE = new CompositeRecordFileReader();

    RecordFile read(StreamFilename filename, byte[] bytes);
}

