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

import com.google.cloud.bigquery.TableId;
import javax.validation.constraints.NotBlank;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Data
@Validated
@ConfigurationProperties("hedera.dedupe")
public class DedupeProperties {

    @NotBlank
    private String projectId;

    @NotBlank
    private String datasetName;

    @NotBlank
    private String transactionsTableName = "transactions";

    @NotBlank
    private String stateTableName = "dedupe_state";

    private boolean metricsEnabled = false;

    private int catchupProbeIntervalSec = 6 * 60 * 60; // 6 hours
    private int steadyStateProbeIntervalSec = 10 * 60; // 10 min

    public String getTransactionsTableFullName() {
        return projectId + "." + datasetName + "." + transactionsTableName;
    }

    public TableId getTransactionsTableId() {
        return TableId.of(projectId, datasetName, transactionsTableName);
    }

    public String getStateTableFullName() {
        return projectId + "." + datasetName + "." + stateTableName;
    }

    public TableId getStateTableId() {
        return TableId.of(projectId, datasetName, stateTableName);
    }
}
