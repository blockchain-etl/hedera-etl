package com.hedera.etl.reader.recordfile.domain.transaction;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.zip.GZIPInputStream;

import org.junit.Test;

import com.hedera.etl.reader.recordfile.domain.StreamFilename;
import com.hedera.etl.reader.recordfile.reader.record.RecordFileReader;

public class RecordFileTest {
  File resourcesDirectory = new File("src/test/resources/synthetic-traffic");

  @Test
  public void testSerialization() {
    var input = testResourcesFrom();

    // Make sure to set this property to see which field had failed to serialize
    // sun.io.serialization.extendedDebugInfo=true

    input.stream()
        .map(
            r -> {
              System.out.println("Serializing " + r.getName());
              ObjectOutputStream out = null;
              try {
                var bytestream = new ByteArrayOutputStream();
                out = new ObjectOutputStream(bytestream);
                out.writeObject(r);
                out.flush();
                out.close();
                return bytestream.toByteArray();
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            })
        .map(
            bytes -> {
              System.out.println("Deserializing");
              try {
                var bytestream = new ByteArrayInputStream(bytes);
                var in = new ObjectInputStream(bytestream);
                var record = (RecordFile) in.readObject();
                in.close();
                return record;
              } catch (IOException | ClassNotFoundException e) {
                throw new RuntimeException(e);
              }
            })
        .toList();
  }

  private List<RecordFile> testResourcesFrom() {
    return Arrays.stream(Objects.requireNonNull(resourcesDirectory.listFiles()))
        .filter(File::isFile)
        .filter(f -> !f.getName().endsWith("_sig"))
        .map(
            file -> {
              System.out.println(file.getPath());
              try {
                return RecordFileReader.INSTANCE.read(
                    StreamFilename.from(file.getPath()),
                    new GZIPInputStream(Files.newInputStream(file.toPath())).readAllBytes());
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            })
        .toList();
  }
}
