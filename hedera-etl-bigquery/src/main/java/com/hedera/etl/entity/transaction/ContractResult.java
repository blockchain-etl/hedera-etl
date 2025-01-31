/*
 * Copyright (C) 2019-2024 Hedera Hashgraph, LLC
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

import java.util.Collections;
import java.util.List;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import com.hedera.etl.reader.recordfile.entity.EntityId;

@Data
@NoArgsConstructor
@SuperBuilder
public class ContractResult {

  private Long amount;

  @ToString.Exclude private byte[] bloom;

  @ToString.Exclude private byte[] callResult;

  private Long consensusTimestamp;

  private long contractId;

  private List<Long> createdContractIds = Collections.emptyList();

  private String errorMessage;

  private byte[] failedInitcode;

  private byte[] functionParameters;

  private byte[]
      functionResult; // Temporary field until we can confirm the migration captured everything

  private Long gasConsumed;

  private Long gasLimit;

  private Long gasUsed;

  private EntityId payerAccountId;

  private EntityId senderId;

  private byte[] transactionHash;

  private Integer transactionIndex;

  private int transactionNonce;

  private Integer transactionResult;

  // public ContractTransactionHash toContractTransactionHash() {
  // return ContractTransactionHash.builder()
  // .consensusTimestamp(consensusTimestamp)
  // .hash(transactionHash)
  // .entityId(contractId)
  // .payerAccountId(payerAccountId.getId())
  // .transactionResult(transactionResult)
  // .build();
  // }
}
