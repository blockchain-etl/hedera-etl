package com.hedera.etl.entity.openaccess;

import javax.annotation.Nullable;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCaseFormat;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.CaseFormat;

import com.hedera.etl.reader.recordfile.domain.transaction.RecordFile;
import com.hedera.etl.util.FormatUtils;

@DefaultSchema(JavaBeanSchema.class)
@SchemaCaseFormat(CaseFormat.LOWER_UNDERSCORE)
@Data
@NoArgsConstructor
@AllArgsConstructor(onConstructor_ = @SchemaCreate)
@Builder
public class Block {
  @Nullable private Long blockHeight;
  private String blockHash;
  @Nullable private String parentBlockHash;
  private String timestamp;
  @Nullable private Long slot;
  @Nullable private Long size;
  @Nullable private Long difficulty;
  @Nullable private Long totalDifficulty;
  @Nullable private Long networkFeeUsed;
  @Nullable private Long networkFeeLimit;
  private String chainSpecific;

  public static Block from(RecordFile file) {
    return builder()
        .blockHeight(file.getIndex())
        .blockHash(file.getHash())
        .parentBlockHash(file.getPreviousHash())
        .timestamp(FormatUtils.timestampFromNanos(file.getConsensusStart()))
        .slot(file.getConsensusStart())
        .size((long) file.getSize())
        .difficulty(null) // N/A
        .totalDifficulty(null) // N/A
        .networkFeeUsed(
            file.getItems().stream()
                .mapToLong(i -> i.getTransactionRecord().getTransactionFee())
                .sum())
        .networkFeeLimit(null) // N/A
        .chainSpecific(
            JsonUtils.serialize(
                ChainSpecific.builder()
                    .recordFileName(file.getName())
                    .hapiVersion(file.getHapiVersion().toString())
                    .build()))
        .build();
  }

  @DefaultSchema(JavaBeanSchema.class)
  @SchemaCaseFormat(CaseFormat.LOWER_UNDERSCORE)
  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  @Builder
  public static class ChainSpecific {
    @Nullable private String recordFileName;
    @Nullable private String hapiVersion;
  }
}
