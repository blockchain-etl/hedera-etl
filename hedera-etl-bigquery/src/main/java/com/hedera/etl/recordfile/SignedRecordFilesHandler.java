package com.hedera.etl.recordfile;

import java.io.Serializable;
import java.nio.channels.Channels;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.ibm.icu.impl.Pair;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.util.StreamUtils;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;

import com.hedera.etl.reader.recordfile.domain.StreamFilename;
import com.hedera.etl.reader.recordfile.domain.transaction.RecordFile;
import com.hedera.etl.reader.recordfile.entity.EntityId;
import com.hedera.etl.reader.recordfile.reader.record.RecordFileReader;
import com.hedera.etl.recordfile.verification.Signature;

@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
@Log4j2
public class SignedRecordFilesHandler implements Serializable {
  private String filename;
  private List<RecordFile> cache;

  public static SignedRecordFilesHandler cache(RecordFileHandler rcdFile) {
    return builder().filename(rcdFile.getFilename()).cache(readFiles(rcdFile)).build();
  }

  private static List<RecordFile> readFiles(RecordFileHandler rcdFile) {
    // group by different signatures
    var signatures =
        rcdFile.getFiles().stream()
            .map(file -> Pair.of(file, getSignatureForFile(file)))
            .filter(p -> p.second.isPresent())
            .collect(Collectors.groupingBy(p -> p.second.get()));

    return signatures.values().stream()
        .flatMap(
            // for each different signature find one matching file,
            // this protects us from valid signature and invalid rcd file
            list ->
                list.stream()
                    .flatMap(
                        // parse record file
                        p -> {
                          var file = p.first;
                          var signature = p.second;
                          try {
                            var recordFile =
                                RecordFileReader.INSTANCE.read(
                                    StreamFilename.from(file.getMetadata().resourceId().toString()),
                                    p.first.readFullyAsBytes());
                            recordFile.setNodeId(
                                EntityId.of(
                                        getNodeFromUrl(file.getMetadata().resourceId().toString()))
                                    .getId());
                            return Stream.of(Pair.of(recordFile, signature));
                          } catch (Exception e) {
                            log.warn(
                                "Can't read Record File %s"
                                    .formatted(file.getMetadata().resourceId().toString()),
                                e);
                            return Stream.empty();
                          }
                        })
                    .filter(
                        // filter by matching signature
                        pair -> {
                          var file = pair.first;
                          var signature = pair.second.get();

                          if (!Objects.equals(file.getFileHash(), signature.getFileHash())) {
                            log.warn(
                                "Mismatch in file {} file hash between {} and {}",
                                file.getName(),
                                file.getFileHash(),
                                signature.getFileHash());
                            return false;
                          }

                          if (!Objects.equals(
                              file.getMetadataHash(), signature.getMetadataHash())) {
                            log.warn(
                                "Mismatch in file {} metadata hash between {} and {}",
                                file.getName(),
                                file.getMetadataHash(),
                                signature.getMetadataHash());
                            return false;
                          }

                          return true;
                        })
                    .map(p -> p.first)
                    .findAny()
                    .stream())
        .toList();
  }

  public static Optional<Signature> getSignatureForFile(FileIO.ReadableFile file) {
    var signaturePath = file.getMetadata().resourceId().toString().replace(".gz", "") + "_sig";

    var signatureResourceId = FileSystems.matchNewResource(signaturePath, false);
    try (var stream = Channels.newInputStream(FileSystems.open(signatureResourceId))) {
      return Optional.of(
          Signature.fromBytes(signaturePath, StreamUtils.getBytesWithoutClosing(stream)));
    } catch (Exception ex) {
      log.warn(
          "Failed to read signature file for %s"
              .formatted(file.getMetadata().resourceId().toString()),
          ex);
      return Optional.empty();
    }
  }

  public Instant timestamp() {
    return Instant.parse(
        this.filename.replace(".rcd", "").replace(".gz", ""),
        DateTimeFormat.forPattern("yyyy-MM-dd'T'HH_mm_ss.SSSSSSSSS'Z'"));
  }

  private static final Pattern URL_NODE_EXTRACTION_PATTERN =
      Pattern.compile(".*/recordstreams/record(\\d+\\.\\d+\\.\\d+)/\\d+.*");

  public static String getNodeFromUrl(String url) {
    var m = URL_NODE_EXTRACTION_PATTERN.matcher(url);
    if (m.matches()) {
      return m.group(1);
    } else {
      return "0.0.0";
    }
  }
}
