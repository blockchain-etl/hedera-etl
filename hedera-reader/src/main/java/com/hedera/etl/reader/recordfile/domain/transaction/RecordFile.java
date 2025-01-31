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

package com.hedera.etl.reader.recordfile.domain.transaction;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;

import lombok.*;

import com.hedera.etl.reader.recordfile.domain.StreamFile;
import com.hedera.etl.reader.recordfile.entity.DigestAlgorithm;
import com.hedera.etl.reader.recordfile.entity.Version;

@Builder(toBuilder = true)
@Data
@AllArgsConstructor
@NoArgsConstructor
public class RecordFile implements StreamFile<RecordItem>, Serializable {

  public static final Version HAPI_VERSION_0_23_0 = new Version(0, 23, 0);
  public static final Version HAPI_VERSION_0_27_0 = new Version(0, 27, 0);
  public static final Version HAPI_VERSION_0_47_0 = new Version(0, 47, 0);
  public static final Version HAPI_VERSION_0_49_0 = new Version(0, 49, 0);
  public static final Version HAPI_VERSION_0_53_0 = new Version(0, 53, 0);
  public static final Version HAPI_VERSION_NOT_SET = new Version(0, 0, 0);

  @ToString.Exclude private byte[] bytes;

  private Long consensusEnd;

  private Long consensusStart;

  private Long count;

  private DigestAlgorithm digestAlgorithm;

  @ToString.Exclude private String fileHash;

  @Builder.Default private long gasUsed = 0L;

  @Getter(lazy = true)
  private final Version hapiVersion = hapiVersion();

  private Integer hapiVersionMajor;
  private Integer hapiVersionMinor;

  private Integer hapiVersionPatch;

  @ToString.Exclude private String hash;

  private Long index;

  @Builder.Default @EqualsAndHashCode.Exclude @ToString.Exclude
  private Collection<RecordItem> items = List.of();

  private Long loadEnd;

  private Long loadStart;

  @ToString.Exclude private byte[] logsBloom;

  // @Getter(PRIVATE)
  // private final AtomicInteger logIndex = new AtomicInteger(0);
  @ToString.Exclude private String metadataHash;

  private String name;

  private Long nodeId;

  @ToString.Exclude private String previousHash;

  private int sidecarCount;

  @Builder.Default @EqualsAndHashCode.Exclude @ToString.Exclude
  private Collection<SidecarFile> sidecars = List.of();

  private Integer size;

  private int version;

  @Override
  public void clear() {
    StreamFile.super.clear();
    setLogsBloom(null);
    // setSidecars(List.of());
  }

  @Override
  public StreamFile<RecordItem> copy() {
    return this.toBuilder().build();
  }

  private Version hapiVersion() {
    if (hapiVersionMajor == null || hapiVersionMinor == null || hapiVersionPatch == null) {
      return HAPI_VERSION_NOT_SET;
    }

    return new Version(hapiVersionMajor, hapiVersionMinor, hapiVersionPatch);
  }
}
