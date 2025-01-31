package com.hedera.etl.recordfile;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.InferableFunction;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

import com.hedera.etl.reader.recordfile.domain.StreamFilename;
import com.hedera.etl.reader.recordfile.domain.transaction.RecordFile;
import com.hedera.etl.reader.recordfile.reader.record.RecordFileReader;

@Slf4j
public class RecordFileTransform
    extends PTransform<PCollection<FileIO.ReadableFile>, PCollection<RecordFile>> {
  @Override
  public PCollection<RecordFile> expand(PCollection<FileIO.ReadableFile> input) {
    return input.apply(
        "Read Record Files",
        MapElements.via(
            new InferableFunction<>() {
              // TODO: add sending errors to Dead Letter Queue
              @SneakyThrows
              @Override
              public RecordFile apply(FileIO.ReadableFile file) {
                log.debug("Parsing file {}", file.getMetadata().resourceId());
                return RecordFileReader.INSTANCE.read(
                    StreamFilename.from(file.getMetadata().resourceId().getFilename()),
                    file.readFullyAsBytes());
              }
            }));
  }
}
