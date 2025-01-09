package com.hedera.etl.recordfile;

import com.hedera.etl.recordfile.domain.StreamFilename;
import com.hedera.etl.recordfile.domain.transaction.RecordFile;

import com.hedera.etl.recordfile.reader.record.RecordFileReader;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.InferableFunction;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

@Slf4j
public class RecordFileTransform extends PTransform<PCollection<FileIO.ReadableFile>, PCollection<RecordFile>> {
    @Override
    public PCollection<RecordFile> expand(PCollection<FileIO.ReadableFile> input) {
        return input
                .apply("Read Record Files", MapElements.via(new InferableFunction<>() {
                    //TODO: add sending errors to Dead Letter Queue
                    @SneakyThrows
                    @Override
                    public RecordFile apply(FileIO.ReadableFile file) {
                        log.debug("Parsing file {}", file.getMetadata().resourceId());
                        return RecordFileReader.INSTANCE.read(StreamFilename.from(file), file.readFullyAsBytes());
                    }
                }));
    }
}
