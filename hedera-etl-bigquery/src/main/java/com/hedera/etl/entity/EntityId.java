package com.hedera.etl.entity;

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
public class EntityId {
  private long shard;
  private long realm;
  private long num;

  public static EntityId from(com.hedera.etl.reader.recordfile.entity.EntityId entityId) {
    return builder()
        .shard(entityId.getShard())
        .realm(entityId.getRealm())
        .num(entityId.getNum())
        .build();
  }

  public String toString() {
    return getShard() + "." + getRealm() + "." + getNum();
  }
}
