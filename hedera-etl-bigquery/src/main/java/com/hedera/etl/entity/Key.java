package com.hedera.etl.entity;

import java.util.Arrays;
import java.util.HexFormat;

import javax.annotation.Nullable;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

import com.hedera.etl.reader.recordfile.utils.DomainUtils;

import static com.hederahashgraph.api.proto.java.Key.KeyCase.KEYLIST;

@DefaultSchema(JavaBeanSchema.class)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Key {

  private static final String PROTOBUF_TYPE = "ProtobufEncoded";
  private static final byte[] EMPTY_KEYLIST = HexFormat.of().parseHex("3200");

  public static Key from(com.hederahashgraph.api.proto.java.Key key) {
    if (key == null) {
      return null;
    }

    if (key.getKeyCase() == KEYLIST && Arrays.equals(EMPTY_KEYLIST, key.toByteArray())) {
      return null;
    }

    return switch (key.getKeyCase()) {
      case KEY_NOT_SET -> null;
      case ED25519, ECDSA_SECP256K1 ->
          builder()
              ._type(key.getKeyCase().name())
              .key(DomainUtils.getPublicKey(key.toByteArray()))
              .build();
      case KEYLIST, THRESHOLDKEY -> {
        var bytes = key.toByteArray();
        if (Arrays.equals(EMPTY_KEYLIST, bytes)) {
          yield null;
        }
        var pubKey = DomainUtils.getPublicKey(bytes);
        // if null it means that it contains more than one key
        if (pubKey == null) {
          yield builder()
              ._type(PROTOBUF_TYPE)
              .key(DomainUtils.bytesToHex(key.toByteArray()))
              .build();
        } else {
          if (key.getKeyCase() == KEYLIST) {
            yield from(key.getKeyList().getKeysList().getFirst());
          } else {
            yield from(key.getThresholdKey().getKeys().getKeysList().getFirst());
          }
        }
      }
      default ->
          builder()._type(PROTOBUF_TYPE).key(DomainUtils.bytesToHex(key.toByteArray())).build();
    };
  }

  @Nullable private String _type;

  @Nullable private String key;
}
