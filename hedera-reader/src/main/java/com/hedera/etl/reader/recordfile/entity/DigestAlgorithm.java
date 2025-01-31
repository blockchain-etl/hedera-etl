package com.hedera.etl.reader.recordfile.entity;

import lombok.Getter;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;

@Getter
public enum DigestAlgorithm {
  SHA_384("SHA-384", 48, 0x58ff811b);

  private final String emptyHash;
  private final String name;
  private final int size;
  private final int type; // as defined in the stream file v5 format document

  DigestAlgorithm(String name, int size, int type) {
    this.name = name;
    this.size = size;
    this.type = type;
    this.emptyHash = Hex.encodeHexString(new byte[size]);
  }

  public boolean isHashEmpty(String hash) {
    return StringUtils.isEmpty(hash) || hash.equals(emptyHash);
  }
}
