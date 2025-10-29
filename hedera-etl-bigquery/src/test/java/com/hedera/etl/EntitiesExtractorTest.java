package com.hedera.etl;

import java.io.File;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.StreamSupport;

import lombok.SneakyThrows;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Sets;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import com.hedera.etl.entity.Block;
import com.hedera.etl.entity.TimestampRange;
import com.hedera.etl.entity.balance.Balance;
import com.hedera.etl.reader.recordfile.domain.transaction.RecordFile;
import com.hedera.etl.recordfile.RecordFileTransform;

import static org.junit.Assert.*;

public class EntitiesExtractorTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  File resourcesDirectory = new File("src/test/resources");

  @Ignore
  @Test
  public void testEntitiesExtractor() {

    // given
    var input = testResourcesFrom("synthetic-traffic");

    // when
    var output = EntitiesExtractor.extract(input, List.of("open.block"));

    // then
    assertThatContainsObject(
        output.restrictedAccess().get("Block"),
        Block.class,
        Block.builder()
            .name("2025-03-21T14_26_39.011502667Z.rcd.gz")
            .count(723L)
            .size(219875L)
            .number(0L)
            .timestamp(
                TimestampRange.builder()
                    .from(1742567199011501945L)
                    .to(1742567199011502667L)
                    .build())
            .hash(
                "c37f9bd54398e59eb90849733228f40aec93028116cf08c8ec64e04fe7dfb80515ed2def87276f416f0c1cf62cce5015")
            .gas_used(0L)
            .hapi_version("0.59")
            .logs_bloom(null)
            .previous_hash(
                "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000")
            .build());

    pipeline.run();
  }

  @Ignore
  @Test
  public void testEntitiesBalance() {

    // given
    var input = testResourcesFrom("synthetic-traffic");

    // when
    var output = EntitiesExtractor.extract(input, List.of("restricted.balance"));

    // then
    assertThatContainsObject(
        output.restrictedAccess().get("Balance"),
        Balance.class,
        Balance.builder()
            .account_id("0.0.98")
            .amount(BigDecimal.valueOf(432564813L))
            .consensus_timestamp(1743509496987991331L)
            .created("2025-04-01T12:11:36.987991Z")
            .build());

    pipeline.run();
  }

  private PCollection<RecordFile> testResourcesFrom(String directory) {
    var pattern = "file://" + resourcesDirectory.getAbsolutePath() + "/" + directory + "/*";

    return pipeline
        .apply(FileIO.match().filepattern(pattern))
        .apply(
            new RecordFileTransform(
                "",
                "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
                true));
  }

  private void assertThatContainsRow(
      PCollection<Row> rows, Function<Row.Builder, Row.FieldValueBuilder> expected) {
    var expectedValue = expected.apply(Row.withSchema(rows.getSchema())).build();

    PAssert.that(rows)
        .satisfies(
            rs -> {
              var actual =
                  StreamSupport.stream(rs.spliterator(), false)
                      .filter(r -> r.equals(expectedValue))
                      .findAny();

              assertTrue(actual.isPresent());

              return null;
            });
  }

  @SneakyThrows
  private <T> void assertThatContainsObject(PCollection<Row> rows, Class<T> clazz, T... expected) {
    var expectedValueCollection = pipeline.apply(Create.of(Arrays.stream(expected).toList()));
    //            .apply(Convert.toRows());

    var filteredRows =
        rows.apply(Convert.fromRows(clazz)).apply(Sets.intersectDistinct(expectedValueCollection));

    PAssert.that(filteredRows).containsInAnyOrder(expected);
  }
}
