package com.hedera.etl;

import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.*;

public class StreamingStorageToBigQueryPipelineTest {
  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testGenerator() {
    // given
    var startingTimestamp = Instant.EPOCH;
    var createInput =
        TestStream.create(KvCoder.of(VarIntCoder.of(), VarLongCoder.of()))
            .advanceWatermarkTo(new Instant(0L).plus(Duration.standardMinutes(1)))
            .addElements(KV.of(0, 1L))
            .advanceWatermarkTo(new Instant(0L).plus(Duration.standardMinutes(2)))
            .addElements(KV.of(0, 2L))
            .advanceWatermarkTo(new Instant(0L).plus(Duration.standardMinutes(3)))
            .addElements(KV.of(0, 3L))
            .advanceWatermarkToInfinity();

    // when
    var result =
        pipeline
            .apply(createInput)
            .apply(
                ParDo.of(
                    new StreamingStorageToBigQueryPipeline.GenerateFileNamesPrefixesByTimestamp(
                        startingTimestamp)));

    // then
    PAssert.that(result)
        .containsInAnyOrder("1970-01-01T00_00", "1970-01-01T00_01", "1970-01-01T00_02");

    pipeline.run();
  }
}
