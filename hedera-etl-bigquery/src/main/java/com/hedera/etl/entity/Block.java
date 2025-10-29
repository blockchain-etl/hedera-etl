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
import com.hedera.etl.util.FormatUtils;

@DefaultSchema(JavaBeanSchema.class)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Block {
  @Nullable private Long count;
  @Nullable private Long gas_used;
  @Nullable private String hapi_version;
  @Nullable private String hash;
  @Nullable private String logs_bloom;
  @Nullable private String name;
  @Nullable private String created;
  @Nullable private Long number;
  @Nullable private String previous_hash;
  @Nullable private Long size;

  @Nullable private TimestampRange timestamp;

  public static Block from(RecordFile recordFile) {
    return Block.builder()
        .count(recordFile.getCount())
        .gas_used(recordFile.getGasUsed())
        .hapi_version(recordFile.getHapiVersion().toString())
        .hash(DomainUtils.HEX_PREFIX + recordFile.getHash())
        .logs_bloom(
            DomainUtils.HEX_PREFIX
                + Optional.ofNullable(recordFile.getLogsBloom())
                    .map(DomainUtils::bytesToHex)
                    .orElse(""))
        .name(recordFile.getName())
        .created(FormatUtils.timestampFromNanos(recordFile.getConsensusStart()))
        .number(recordFile.getIndex())
        .previous_hash(DomainUtils.HEX_PREFIX + recordFile.getPreviousHash())
        .size(recordFile.getSize().longValue())
        .timestamp(
            TimestampRange.builder()
                .from(recordFile.getConsensusStart())
                .to(recordFile.getConsensusEnd())
                .build())
        .build();
  }
}
