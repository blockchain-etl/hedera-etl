package com.hedera.etl.recordfile;

import org.junit.Test;

import static org.junit.Assert.*;

public class SignedRecordFilesHandlerTest {

  @Test
  public void testNodeExtractionFromUrl() {
    var input = "gs://bucket/recordstreams/record0.0.3/2020-01-01T10_00_00.rcd.gz";

    var result = SignedRecordFilesHandler.getNodeFromUrl(input);

    assertEquals("0.0.3", result);
  }

  @Test
  public void testNodeExtractionFromWrongUrl() {
    var input = "random://url";

    var result = SignedRecordFilesHandler.getNodeFromUrl(input);

    assertEquals("0.0.0", result);
  }
}
