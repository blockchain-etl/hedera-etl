package com.hedera.etl.recordfile;

import com.hedera.etl.recordfile.entity.RecordFile;

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
                        return RecordFileReader.parse(file.readFullyAsBytes());
                    }
                }));
    }
}
