package com.hedera.etl.entity.smartcontracts;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

import javax.annotation.Nullable;

//TODO: merge with Key from Token

@DefaultSchema(JavaBeanSchema.class)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Key {
    @Nullable
    private KeyType _type;
    @Nullable
    private byte[] key;

    public enum KeyType {
        ECDSA_SECP256K1, ED25519, ProtobufEncoded
    }

    public static Key from(com.hederahashgraph.api.proto.java.Key key) {
      return Key.builder()
              ._type(KeyType.ProtobufEncoded)
              .key(key.toByteArray())
              .build();
    }
}
