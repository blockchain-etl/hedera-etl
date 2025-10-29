package com.hedera.etl.entity.topic;

import javax.annotation.Nullable;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

@DefaultSchema(JavaBeanSchema.class)
@NoArgsConstructor
@AllArgsConstructor
@Data
@Builder
public class ChunkInfo {
  @Nullable private InitialTransactionId initial_transaction_id;
  @Nullable private Integer number;
  @Nullable private Integer total;

  @DefaultSchema(JavaBeanSchema.class)
  @NoArgsConstructor
  @AllArgsConstructor
  @Data
  @Builder
  public static class InitialTransactionId {
    @Nullable private String account_id;
    @Nullable private Integer nonce;
    @Nullable private Long transaction_valid_start;
    @Nullable private Boolean scheduled;
  }
}
