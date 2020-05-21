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

/**
 * FullDeduplication does deduplication of all transactions before 'endTimestamp' of last successful incremental run.
 * <p/>
 * Need for full deduplication:
 * <p/>
 * It takes about an hour or more for a newly inserted transaction to make it from streaming buffer to regular
 * partitions (accessible by DML operations). Incremental deduplication logic assumes that transactions can arrive out
 * of order, however, they won't be delayed by as much as an hour. <br>
 * That is, if deduplication logic has checked data up to point X where X = (now - 90min), then there would be no new
 * transaction with consensusTimestamp < X. This is fair assumption since in a normally functioning system, delays
 * due to PubSub, or transient errors in Dataflow, or transient errors in BigQuery streaming inserts won't be more
 * than few minutes. <br>
 * Above assumption also incorporates in itself the fact that no duplicates should arrive as late as an hour.
 * <p/>
 * However, above assumption breaks down when the system is not functioning normally. To list some of the scenarios:
 * <ul>
 *   <li>If importer (publishing to pubsub) crashes. It can publish duplicates when it comes back up much later</li>
 *   <li>If dataflow pipeline crashes due to a bug. It'll likely take more than an hour to fix it and restart
 *   the pipeline.</li>
 * </ul>
 * To cover such scenarios, running an infrequent job which does deduplication over all past data is necessary.
 * <p/>
 * There is a limit of 4000 partitions that can be updated by a single DML operation. That's more than 10 years of data,
 * so we don't need to worry about it for now.
 */
@Component
public class FullDeduplication extends AbstractDeduplication {

    public FullDeduplication(DedupeProperties properties, BigQuery bigQuery, MeterRegistry meterRegistry) {
        super(DedupeType.FULL, properties, bigQuery, meterRegistry);
    }

    @Scheduled(fixedRateString = "${hedera.dedupe.fullFixedRate:86400000}") // default: 24 hr
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
        setState.set(FULL_LATEST_END_TIMESTAMP, String.valueOf(timestampWindow.getEndTimestamp()));
    }
}
