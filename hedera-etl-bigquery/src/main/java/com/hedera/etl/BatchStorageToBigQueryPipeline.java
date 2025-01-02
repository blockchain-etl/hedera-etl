package com.hedera.etl;

/*-
 * ‌
 * Hedera ETL
 * ​
 * Copyright (C) 2020 Hedera Hashgraph, LLC
 * ​
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
 * ‍
 */

import com.hedera.etl.recordfile.RecordFileTransform;

import lombok.RequiredArgsConstructor;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;

@RequiredArgsConstructor
public class BatchStorageToBigQueryPipeline {

    private final BatchStorageToBigQueryPipelineOptions options;

    void run() {
        var pipeline = Pipeline.create(options);
        var pcollection = pipeline
                .apply("List files", FileIO.match().filepattern(options.getInputPathPattern()))
                .apply("Read files", FileIO.readMatches())
                .apply("Parse Record Files", new RecordFileTransform());
        pipeline.run();
    }
}
