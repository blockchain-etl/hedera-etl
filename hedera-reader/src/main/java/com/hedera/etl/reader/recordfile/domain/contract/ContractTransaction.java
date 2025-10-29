/*
 * Copyright (C) 2023-2024 Hedera Hashgraph, LLC
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

package com.hedera.etl.reader.recordfile.domain.contract;

import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import com.hedera.etl.reader.recordfile.converter.ListToStringSerializer;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ContractTransaction {
  private Long consensusTimestamp;

  @Builder.Default
  @JsonSerialize(using = ListToStringSerializer.class)
  private List<Long> contractIds = new ArrayList<>();

  private Long entityId;

  private long payerAccountId;

  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  public static class Id implements Serializable {
    @Serial private static final long serialVersionUID = -6807023295883699004L;

    private long consensusTimestamp;
    private long entityId;
  }
}
