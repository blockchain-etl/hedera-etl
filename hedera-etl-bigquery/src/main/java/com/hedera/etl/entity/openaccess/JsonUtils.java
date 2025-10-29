package com.hedera.etl.entity.openaccess;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;

@UtilityClass
public class JsonUtils {
  private static final ObjectMapper mapper = new ObjectMapper();

  @SneakyThrows
  public static String serialize(Object o) {
    return mapper.writeValueAsString(o);
  }
}
