package com.hedera.etl.recordfile;

import java.util.Arrays;
import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Comparators;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.kryo.KryoCoder;
import org.apache.beam.sdk.io.ReadMatchesWithChecksum;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Duration;

import com.hedera.etl.reader.recordfile.domain.transaction.RecordFile;
import com.hedera.etl.util.FormatUtils;

@Slf4j
public class RecordFileTransform
    extends PTransform<PCollection<MatchResult.Metadata>, PCollection<RecordFile>> {

  private final String startAboveFilenameWithoutExtension;
  private final String lastValidHash;
  private final boolean enableVerification;

  public RecordFileTransform(String startAboveFilename, String lastValidHash) {
    this(startAboveFilename, lastValidHash, false);
  }

  @VisibleForTesting
  public RecordFileTransform(
      String startAboveFilename, String lastValidHash, boolean enableVerification) {
    this.startAboveFilenameWithoutExtension =
        FormatUtils.removeSuffixes(startAboveFilename, ".rcd", ".rcd.gz");
    this.lastValidHash = lastValidHash;
    this.enableVerification = enableVerification;
  }

  @Override
  public PCollection<RecordFile> expand(PCollection<MatchResult.Metadata> input) {
    var rcdFiles =
        input
            .apply("Filter .rcd files", filterByExtension(".rcd", ".rcd.gz"))
            //            .apply("Read .rcd files", FileIO.readMatches())
            .apply("Read .rcd files", new ReadMatchesWithChecksum())
            .apply(
                "Add nodes to values",
                MapElements.into(TypeDescriptor.of(RecordFileHandler.class))
                    .via(
                        file ->
                            RecordFileHandler.builder()
                                .filename(file.getMetadata().resourceId().getFilename())
                                .file(file)
                                .build()))
            .setCoder(KryoCoder.of())
            .apply(
                "Add timestamps",
                WithTimestamps.of(RecordFileHandler::timestamp)
                    .withAllowedTimestampSkew(new Duration(Long.MAX_VALUE)));

    return rcdFiles
//        .apply(
//            "Add grouping window",
//            Window.<RecordFileHandler>into(FixedWindows.of(Duration.standardMinutes(1)))
//                .discardingFiredPanes()
//                .withAllowedLateness(
//                    Duration.standardHours(1), Window.ClosingBehavior.FIRE_IF_NON_EMPTY)
//                .withTimestampCombiner(TimestampCombiner.EARLIEST))
//        .apply("Key by name", WithKeys.of((RecordFileHandler file) -> file.getFilename()))
//        .setCoder(
//            KvCoder.of(
//                NullableCoder.of(StringUtf8Coder.of()), NullableCoder.of(rcdFiles.getCoder())))
//        .apply("Group by filename", Combine.perKey(RecordFileHandler::merge))
//        .apply("Reify window to global", Window.into(new GlobalWindows()))
//        .apply("Drop filenames", Values.create())
        .apply(
            "Verify signature and parse matching files",
            FlatMapElements.into(TypeDescriptor.of(SignedRecordFilesHandler.class))
                .via(
                    rcdFile ->
                        Optional.ofNullable(rcdFile).map(SignedRecordFilesHandler::cache).stream()
                            .toList()))
        .apply(
            "Add timestamps",
            WithTimestamps.of(SignedRecordFilesHandler::timestamp)
                .withAllowedTimestampSkew(new Duration(Long.MAX_VALUE)))
        .apply("Add fake key", WithKeys.of(0))
        .apply("Read Record Files", ParDo.of(new Validator.ValidatorDoFn(lastValidHash)))
        .apply(
            "Add timestamps",
            WithTimestamps.<RecordFile>of(
                    file -> FormatUtils.jodaInstantFromNanos(file.getConsensusStart()))
                .withAllowedTimestampSkew(new Duration(Long.MAX_VALUE)));
  }

  private Filter<MatchResult.Metadata> filterByExtension(String... ext) {
    return Filter.by(
        metadata -> {
          var filename = metadata.resourceId().getFilename();
          var greaterFileName =
              Comparators.max(
                  FormatUtils.removeSuffixes(filename, ext), startAboveFilenameWithoutExtension);
          return (Arrays.stream(ext)).anyMatch(e -> filename.endsWith(e))
              && filename.startsWith(greaterFileName);
        });
  }
}
