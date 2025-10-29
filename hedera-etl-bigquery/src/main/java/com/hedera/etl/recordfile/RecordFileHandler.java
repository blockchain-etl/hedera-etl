package com.hedera.etl.recordfile;

import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.Sets;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.kryo.KryoCoder;
import org.apache.beam.sdk.io.FileIO;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;

@DefaultCoder(KryoCoder.class)
@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
@Log4j2
public class RecordFileHandler {
  private String filename;
  private Set<FileIO.ReadableFile> files;

  public Instant timestamp() {
    return Instant.parse(
        this.filename.replace(".rcd", "").replace(".gz", ""),
        DateTimeFormat.forPattern("yyyy-MM-dd'T'HH_mm_ss.SSSSSSSSS'Z'"));
  }

  RecordFileHandler merge(RecordFileHandler other) {
    return RecordFileHandler.builder()
        .filename(this.filename)
        .files(new HashSet<>(Sets.union(this.files, other.files)))
        .build();
  }

  public static class RecordFileHandlerBuilder {
    public RecordFileHandlerBuilder file(FileIO.ReadableFile file) {
      if (this.files == null) {
        this.files = new HashSet<>();
      }
      // workaround: checksum might be null, though files that been tempered with might have
      // different sizes
      this.files.add(file);
      return this;
    }
  }
}
