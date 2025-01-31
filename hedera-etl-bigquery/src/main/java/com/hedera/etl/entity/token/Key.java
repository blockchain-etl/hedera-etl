package com.hedera.etl.entity.token;

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
public class Key {
  public static Key from(com.hederahashgraph.api.proto.java.Key key) {
    return Key.builder()._type(KeyType.ProtobufEncoded).key(key.toByteArray()).build();
  }

  @Nullable private KeyType _type;

  @Nullable private byte[] key;

  public enum KeyType {
    ECDSA_SECP256K1,
    ED25519,
    ProtobufEncoded
  }
}
