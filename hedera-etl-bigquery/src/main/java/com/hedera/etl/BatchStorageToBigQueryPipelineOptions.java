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

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Hidden;
import org.apache.beam.sdk.options.PipelineOptions;
import org.checkerframework.checker.nullness.qual.NonNull;
import java.time.LocalDate;

public interface BatchStorageToBigQueryPipelineOptions extends PipelineOptions {
    @Description("GCS bucket with Record Files")
    String getInputBucket();
    void setInputBucket(String value);

    @Description("Date from which ingest Record files")
    LocalDate getIngestionDate();
    void setIngestionDate(LocalDate value);

    @Description("The file path to consume from. The name should be in the format of " +
            "filesystem://path/to/files")
    @Hidden
    @Default.InstanceFactory(InputPatternFactory.class)
    String getInputPathPattern();
    void setInputPathPattern(String value);

    class InputPatternFactory implements DefaultValueFactory<String> {
        public String create(@NonNull PipelineOptions pipelineOptions) {
            var options = pipelineOptions.as(BatchStorageToBigQueryPipelineOptions.class);
            return "gs://" + options.getInputBucket() + "/recordstreams/records*/" + options.getIngestionDate() + "*.{rcd,rcd.gz}";
        }
    }

    @Description("Output dataset for entities")
    String getOutputDataset();
    void setOutputDataset(String value);

    @Hidden
    @Description("Disable mere history input")
    boolean getDisableMergeHistoryInput();
    void setDisableMergeHistoryInput(boolean value);

}
