package com.hedera.etl.entity;

import java.util.HexFormat;

import lombok.SneakyThrows;
import org.junit.Test;

import static org.junit.Assert.*;

public class KeyTest {
  @Test
  public void canParseListKey() {
    var hex =
        "329b010a98012a950108021290010a2212209fac973fd48f4a3f51616e362a2248302438e0b2e6d91b965b670fc8b0b13c9f0a221220c8c29cecb3a681f9daba7681fc26c824fb336e9e2a65200682d21e05fd54d3560a2212201bbc982401ca83f91d1250b4830621ed28ea4e6ca7044bf7a59b9d94349a1e340a2212204ec8a4e59231eafabe95a90c637761dd7697f7adf6e899012996d3bb58e9ca9a";
    var hederaKey = hederaKeyFromHex(hex);

    var result = Key.from(hederaKey);

    assertEquals("ProtobufEncoded", result.get_type());
    assertEquals(hex, result.getKey());
  }

  @Test
  public void canParseEmptyKeyList() {
    var hex = "3200";
    var hederaKey = hederaKeyFromHex(hex);

    var result = Key.from(hederaKey);

    assertNull(result);
  }

  @Test
  public void canParseNull() {
    var result = Key.from(null);

    assertNull(result);
  }

  @Test
  public void canParseNotSetKey() {
    var result = Key.from(com.hederahashgraph.api.proto.java.Key.getDefaultInstance());

    assertNull(result);
  }

  @Test
  public void canParseEmptyKey() {
    var result = Key.from(hederaKeyFromHex(""));

    assertNull(result);
  }

  @SneakyThrows
  private com.hederahashgraph.api.proto.java.Key hederaKeyFromHex(String hex) {
    var bytes = HexFormat.of().parseHex(hex);
    return com.hederahashgraph.api.proto.java.Key.parseFrom(bytes);
  }
}
