package com.hedera.etl.diff;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import com.google.api.services.bigquery.model.TableReference;
import lombok.experimental.UtilityClass;

import com.hedera.etl.util.FormatUtils;

@UtilityClass
public class HistoryUtil {
  public static <T> TableReference getTableForSuffix(
      String dataset, String entityName, String suffix) {
    var table = "%s_%s".formatted(entityName.toLowerCase(), suffix);

    return new TableReference().setDatasetId(dataset).setTableId(table);
  }

  public static <T> TableReference getTableFor(
      String dataset, String entityName, LocalDate ingestionDate) {
    return getTableForSuffix(
        dataset, entityName, ingestionDate.format(DateTimeFormatter.ofPattern("yyyyMMdd")));
  }

  public static <T> TableReference getTableForYear(
      String dataset, String tableName, String timestamp) {
    return getTableForSuffix(
        dataset,
        tableName,
        Integer.toString(FormatUtils.localDateFromTimestamp(timestamp).getYear()));
  }
}
