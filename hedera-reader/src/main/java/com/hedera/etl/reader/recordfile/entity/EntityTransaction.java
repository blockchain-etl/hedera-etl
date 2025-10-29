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

package com.hedera.etl.reader.recordfile.entity;

import java.io.Serial;
import java.io.Serializable;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor(access = AccessLevel.PRIVATE) // For Builder
@Builder
@Data
@NoArgsConstructor
public class EntityTransaction {

  private Long consensusTimestamp;

  private Long entityId;

  private EntityId payerAccountId;

  private Integer result;

  private Integer type;

  @AllArgsConstructor
  @Data
  @NoArgsConstructor
  public static class Id implements Serializable {

    @Serial private static final long serialVersionUID = -3010905088908209508L;

    private long consensusTimestamp;
    private long entityId;
  }
}
