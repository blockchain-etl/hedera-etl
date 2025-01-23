package com.hedera.etl.diff;

import com.google.api.services.bigquery.model.TableReference;

import com.hedera.etl.entity.Block;

import org.junit.Test;

import java.time.LocalDate;

import static org.junit.Assert.*;

public class HistoryUtilTest {

  @Test
  public void testHistoryUtil() {
    // given

    // when
    var result = HistoryUtil.getTableFor("test_dataset", Block.class.getSimpleName(), LocalDate.of(2025, 2, 1));

    // then
    assertEquals(new TableReference()
                    .setDatasetId("test_dataset")
                    .setTableId("block_20250201"),
            result
    );
  }
}
