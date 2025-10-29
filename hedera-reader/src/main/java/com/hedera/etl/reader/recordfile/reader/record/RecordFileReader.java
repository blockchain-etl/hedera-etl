package com.hedera.etl.reader.recordfile.reader.record;

import com.hedera.etl.reader.recordfile.domain.StreamFilename;
import com.hedera.etl.reader.recordfile.domain.transaction.RecordFile;

public interface RecordFileReader {
  RecordFileReader INSTANCE = new CompositeRecordFileReader();

  int MAX_TRANSACTION_LENGTH = 64 * 1024;

  RecordFile read(StreamFilename filename, byte[] bytes);
}
