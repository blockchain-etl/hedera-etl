package com.hedera.etl.entity;

import lombok.Builder;
import lombok.Data;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import javax.annotation.Nullable;

@DefaultSchema(JavaBeanSchema.class)
@Data
@Builder
public class ChunkInfo {
  @Nullable
  private byte[] initial_transaction_id;
  @Nullable
  private Integer number;
  @Nullable
  private Integer nonce;
  @Nullable
  private Integer total;
  @Nullable
  private boolean scheduled;
}
