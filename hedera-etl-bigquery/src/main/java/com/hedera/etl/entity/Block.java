package com.hedera.etl.entity;

import java.util.Optional;

import javax.annotation.Nullable;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

import com.hedera.etl.reader.recordfile.domain.transaction.RecordFile;
import com.hedera.etl.reader.recordfile.utils.DomainUtils;

@DefaultSchema(JavaBeanSchema.class)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Block {
  public static Block from(RecordFile item) {
    return Block.builder()
        .count(item.getCount())
        .gas_used(item.getGasUsed())
        .hapi_version(item.getHapiVersion().toString())
        .hash(item.getHash())
        .logs_bloom(
            Optional.ofNullable(item.getLogsBloom()).map(DomainUtils::bytesToHex).orElse(null))
        .name(item.getName())
        .number(item.getIndex())
        .previous_hash(item.getPreviousHash())
        .size(item.getSize().longValue())
        .timestamp(
            TimestampRange.builder()
                .from(item.getConsensusStart())
                .to(item.getConsensusEnd())
                .build())
        .build();
  }

  @Nullable private Long count;
  @Nullable private Long gas_used;
  @Nullable private String hapi_version;
  @Nullable private String hash;
  @Nullable private String logs_bloom;
  @Nullable private String name;
  @Nullable private Long number;
  @Nullable private String previous_hash;
  @Nullable private Long size;

  @Nullable private TimestampRange timestamp;
}
