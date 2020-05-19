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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;

public class Utility {

    public static String getResource(String path) {
        try {
            return Files.readString(Paths.get(Utility.class.getClassLoader().getResource(path).getPath()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Counter getCounter(String name) {
        return Metrics.counter(PubSubToBigQueryPipeline.class, name);
    }

    public static Distribution getDistribution(String name) {
        return Metrics.distribution(PubSubToBigQueryPipeline.class, name);
    }
}
