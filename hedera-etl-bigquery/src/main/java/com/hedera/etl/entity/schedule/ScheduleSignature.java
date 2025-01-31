package com.hedera.etl.entity.schedule;

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
public class ScheduleSignature {

  @Nullable private Long consensus_timestamp;
  @Nullable private byte[] public_key_prefix;
  @Nullable private byte[] signature;
  @Nullable private String type;
  @Nullable private String value;
}
