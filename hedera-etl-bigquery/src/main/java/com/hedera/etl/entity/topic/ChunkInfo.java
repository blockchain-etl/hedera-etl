package com.hedera.etl.entity.topic;

import javax.annotation.Nullable;

import lombok.Builder;
import lombok.Data;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

@DefaultSchema(JavaBeanSchema.class)
@Data
@Builder
public class ChunkInfo {
  @Nullable private byte[] initial_transaction_id;
  @Nullable private Integer nonce;
  @Nullable private Integer number;
  @Nullable private boolean scheduled;
  @Nullable private Integer total;
}
