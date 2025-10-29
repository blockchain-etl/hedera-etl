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

import java.util.List;
import java.util.stream.Collectors;

import com.hederahashgraph.api.proto.java.CustomFeeLimit;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

import com.hedera.etl.reader.recordfile.entity.EntityId;

@DefaultSchema(JavaBeanSchema.class)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class MaxCustomFee {
  private String account_id;
  private Long amount;
  private String denominating_token_id;

  public static List<MaxCustomFee> from(CustomFeeLimit limit) {
    return limit.getFeesList().stream()
        .map(
            fee ->
                builder()
                    .account_id(EntityId.of(limit.getAccountId()).toString())
                    .amount(fee.getAmount())
                    .denominating_token_id(EntityId.of(fee.getDenominatingTokenId()).toString())
                    .build())
        .collect(Collectors.toList());
  }
}
