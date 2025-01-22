/*
 * Copyright (C) 2019-2025 Hedera Hashgraph, LLC
 *
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
 */

package com.hedera.etl.entity.transaction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import com.hedera.etl.recordfile.entity.EntityId;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import java.io.Serial;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;

@DefaultSchema(JavaBeanSchema.class)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AssessedCustomFee {

    private long amount;

    private long collector_account_id;

    private long consensus_timestamp;

    private List<Long> effective_payer_account_ids = Collections.emptyList();

    private EntityId token_id;

    private EntityId payer_account_id;
}
