package com.hedera.etl.entity;

import com.hedera.etl.recordfile.domain.transaction.RecordFile;

import com.hedera.etl.recordfile.utils.DomainUtils;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import javax.annotation.Nullable;
import java.util.Optional;

@DefaultSchema(JavaBeanSchema.class)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Block {
  @Nullable
  private Long count;
  @Nullable
  private Long gas_used;
  @Nullable
  private String hapi_version;
  @Nullable
  private String hash;
  @Nullable
  private String logs_bloom;
  @Nullable
  private String name;
  @Nullable
  private Long number;
  @Nullable
  private String previous_hash;
  @Nullable
  private Long size;
  @Nullable
  private TimestampRange timestamp;

  public static Block from(RecordFile item) {
    return Block.builder()
            .count(item.getCount())
            .gas_used(item.getGasUsed())
            .hapi_version(item.getHapiVersion().toString())
            .hash(item.getHash())
            .logs_bloom(Optional.ofNullable(item.getLogsBloom()).map(DomainUtils::bytesToHex).orElse(null))
            .name(item.getName())
            .number(item.getIndex())
            .previous_hash(item.getPreviousHash())
            .size(item.getSize().longValue())
            .timestamp(TimestampRange.builder()
                    .from(item.getConsensusStart())
                    .to(item.getConsensusEnd())
                    .build()
            )
            .build();
  }
}
