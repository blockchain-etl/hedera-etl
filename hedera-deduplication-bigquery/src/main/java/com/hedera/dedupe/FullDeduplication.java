package com.hedera.dedupe;

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

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.FieldValue;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Map;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class FullDeduplication extends AbstractDeduplication {

    public FullDeduplication(DedupeProperties properties, BigQuery bigQuery, MeterRegistry meterRegistry) {
        super(DedupeType.FULL, properties, bigQuery, meterRegistry);
    }

    // TODO: lookup
    @Scheduled(fixedRateString = "${hedera.dedupe.fixedRate:300000}") // default: 5 min
    public void run() {
        runDedupe();
    }

    @Override
    TimestampWindow getTimestampWindow(Map<String, FieldValue> state) {
        // look at everything before incremental deduplication's last checkpoint.
        long endTimestamp = 0;
        if (state.containsKey(INCREMENTAL_LATEST_END_TIMESTAMP)) {
            endTimestamp = state.get(INCREMENTAL_LATEST_END_TIMESTAMP).getLongValue();
        }
        return new TimestampWindow(0, endTimestamp);
    }

    @Override
    void saveState(TimestampWindow timestampWindow) throws InterruptedException {
        setStateQuery.run(FULL_LATEST_END_TIMESTAMP, String.valueOf(timestampWindow.getEndTimestamp()));
    }
}
