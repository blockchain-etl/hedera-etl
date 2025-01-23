package com.hedera.etl.diff;

import com.google.api.services.bigquery.model.TableReference;
import lombok.experimental.UtilityClass;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

@UtilityClass
public class HistoryUtil {
  public static <T> TableReference getTableFor(String dataset, String entityName, LocalDate ingestionDate) {
    var table = "%s_%s".formatted(
            entityName.toLowerCase(),
            ingestionDate.format(DateTimeFormatter.ofPattern("yyyyMMdd"))
    );

    return new TableReference()
            .setDatasetId(dataset)
            .setTableId(table);
  }
}
