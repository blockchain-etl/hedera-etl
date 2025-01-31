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
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

public class HederaETLApplication {

  public static void main(String[] args) {
    var applicationOptions =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .withoutStrictParsing()
            .as(ApplicationOptions.class);

    switch (applicationOptions.getMode()) {
      case BATCH:
        var options =
            PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .withoutStrictParsing()
                .as(BatchStorageToBigQueryPipelineOptions.class);
        new BatchStorageToBigQueryPipeline(options).run();
        break;
      case REALTIME:
        throw new UnsupportedOperationException("Realtime mode is unsupported for now");
      default:
        throw new UnsupportedOperationException("Unknown mode " + applicationOptions.getMode());
    }
  }

  public interface ApplicationOptions extends PipelineOptions {
    @Description("Which mode to use")
    @Default.Enum("BATCH") // TODO: remove default value after implementation of realtime pipeline
    Mode getMode();

    void setMode(Mode value);

    enum Mode {
      BATCH,
      REALTIME,
    }
  }
}
