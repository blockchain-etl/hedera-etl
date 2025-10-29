package org.apache.beam.sdk.io;

import lombok.Getter;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PCollection;

@Getter
public class ReadMatchesWithChecksum
    extends PTransform<PCollection<MatchResult.Metadata>, PCollection<FileIO.ReadableFile>> {

  private final Compression compression = Compression.AUTO;

  private final DirectoryTreatment directoryTreatment = DirectoryTreatment.SKIP;

  public PCollection<FileIO.ReadableFile> expand(PCollection<MatchResult.Metadata> input) {
    return (PCollection) input.apply(ParDo.of(new ToReadableFileFn(this)));
  }

  public void populateDisplayData(DisplayData.Builder builder) {
    builder.add(DisplayData.item("compression", compression.toString()));
    builder.add(DisplayData.item("directoryTreatment", this.getDirectoryTreatment().toString()));
  }

  static boolean shouldSkipDirectory(
      MatchResult.Metadata metadata, DirectoryTreatment directoryTreatment) {
    if (metadata.resourceId().isDirectory()) {
      switch (directoryTreatment) {
        case SKIP:
          return true;
        case PROHIBIT:
          throw new IllegalArgumentException(
              "Trying to read " + metadata.resourceId() + " which is a directory");
        default:
          throw new UnsupportedOperationException(
              "Unknown DirectoryTreatment: " + directoryTreatment);
      }
    } else {
      return false;
    }
  }

  static FileIO.ReadableFile matchToReadableFile(
      MatchResult.Metadata metadata, Compression compression) {
    compression =
        compression == Compression.AUTO
            ? Compression.detect(metadata.resourceId().getFilename())
            : compression;
    return new FileIO.ReadableFile(
        MatchResult.Metadata.builder()
            .setResourceId(metadata.resourceId())
            .setChecksum(metadata.checksum())
            .setSizeBytes(metadata.sizeBytes())
            .setLastModifiedMillis(metadata.lastModifiedMillis())
            .setIsReadSeekEfficient(
                metadata.isReadSeekEfficient() && compression == Compression.UNCOMPRESSED)
            .build(),
        compression);
  }

  public static enum DirectoryTreatment {
    SKIP,
    PROHIBIT;
  }

  private static class ToReadableFileFn extends DoFn<MatchResult.Metadata, FileIO.ReadableFile> {
    private final ReadMatchesWithChecksum spec;

    private ToReadableFileFn(ReadMatchesWithChecksum spec) {
      this.spec = spec;
    }

    @ProcessElement
    public void process(DoFn<MatchResult.Metadata, FileIO.ReadableFile>.ProcessContext c) {
      if (!shouldSkipDirectory(c.element(), this.spec.getDirectoryTreatment())) {
        FileIO.ReadableFile r = matchToReadableFile(c.element(), this.spec.getCompression());
        c.output(r);
      }
    }
  }
}
