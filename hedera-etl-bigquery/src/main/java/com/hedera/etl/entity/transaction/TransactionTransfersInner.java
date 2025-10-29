package com.hedera.etl.entity.transaction;

import javax.annotation.Nullable;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

@DefaultSchema(JavaBeanSchema.class)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TransactionTransfersInner {
  @Nullable private String account;
  @Nullable private Long amount;
  @Nullable private Boolean is_approval;
  @Nullable private ErrataType errata;
}
