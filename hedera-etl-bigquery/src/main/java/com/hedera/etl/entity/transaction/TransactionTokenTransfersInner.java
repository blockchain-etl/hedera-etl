package com.hedera.etl.entity.transaction;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import javax.annotation.Nullable;

@DefaultSchema(JavaBeanSchema.class)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TransactionTokenTransfersInner {
  @Nullable
  private String token_id;
  @Nullable
  private String account;
  @Nullable
  private Long amount;
  @Nullable
  private Boolean is_approval;
}
