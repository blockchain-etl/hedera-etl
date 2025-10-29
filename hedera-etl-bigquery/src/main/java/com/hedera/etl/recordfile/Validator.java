package com.hedera.etl.recordfile;

import java.util.ArrayList;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import com.google.common.collect.Iterables;
import lombok.extern.log4j.Log4j2;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.OrderedListState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.commons.lang3.stream.Streams;
import org.jetbrains.annotations.NotNull;
import org.joda.time.Duration;
import org.joda.time.Instant;

import com.hedera.etl.reader.recordfile.domain.transaction.RecordFile;
import com.hedera.etl.reader.recordfile.entity.EntityId;

@Log4j2
public class Validator {
  public static class ValidatorDoFn
      extends DoFn<KV<Integer, ? extends SignedRecordFilesHandler>, RecordFile> {

    public ValidatorDoFn(String lastValidHash) {
      this.lastValidHash = lastValidHash;
    }

    @StateId("previousFiles")
    private final StateSpec<MapState<Instant, SignedRecordFilesHandler>> previousFiles =
        StateSpecs.map(InstantCoder.of(), SerializableCoder.of(SignedRecordFilesHandler.class));

    @StateId("lastHash")
    private final StateSpec<ValueState<String>> lastHash =
        StateSpecs.value(NullableCoder.of(StringUtf8Coder.of()));

    @StateId("lastTs")
    private final StateSpec<ValueState<Instant>> lastTs =
        StateSpecs.value(NullableCoder.of(InstantCoder.of()));

    @StateId("lastFilename")
    private final StateSpec<ValueState<String>> lastFilename =
        StateSpecs.value(NullableCoder.of(StringUtf8Coder.of()));

    @TimerId("validationTimer")
    private final TimerSpec validationTimer = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    @StateId("timerSet")
    private final StateSpec<ValueState<Boolean>> timerSet = StateSpecs.value();

    private final String lastValidHash;

    @ProcessElement
    public void processElement(
        ProcessContext context,
        @StateId("previousFiles") MapState<Instant, SignedRecordFilesHandler> previousFiles,
        @StateId("timerSet") ValueState<Boolean> timerSet,
        @TimerId("validationTimer") Timer timer) {
      var value = context.element().getValue();
      var timestamp = value.timestamp();
      previousFiles.put(timestamp, value);

      log.debug(
          "Got file {} on nodes {}",
          value.getFilename(),
          value.getCache().stream()
              .map(
                  rcd ->
                      Optional.ofNullable(rcd.getNodeId())
                          .map(EntityId::of)
                          .orElse(EntityId.EMPTY)
                          .toString())
              .toList());

      if (timerSet.read() != Boolean.TRUE) {
        timer.offset(Duration.standardSeconds(60)).setRelative();
        timerSet.write(true);
      }
    }

    @NotNull @Override
    public Duration getAllowedTimestampSkew() {
      return new Duration(Long.MAX_VALUE);
    }

    @OnTimer("validationTimer")
    public void onValidationTimer(
        OnTimerContext context,
        @StateId("previousFiles") MapState<Instant, SignedRecordFilesHandler> previousFiles,
        @StateId("lastTs") ValueState<Instant> lastTsState,
        @StateId("lastFilename") ValueState<String> lastFilenameState,
        @StateId("lastHash") ValueState<String> lastHashState,
        @StateId("timerSet") ValueState<Boolean> timerSet) {
      log.info("Starting validation/emission round: {}", context.timestamp());
      var timestamps = Streams.of(previousFiles.keys().read()).sorted().toList();
      log.info("Timestamps in state:\n{}", timestamps);

      var firstTs = lastTsState.read();
      var lastTs = lastTsState.read();

      var lastHash = new AtomicReference<>(lastHashState.read());
      if (lastHash.get() == null) {
        lastHash.set(lastValidHash);
      }

      var lastFilename = lastFilenameState.read();

      String badFile = null;

      int emittedElements = 0;

      for (var ts : timestamps) {
        if (emittedElements >= 10000) {
          break;
        }

        var value = previousFiles.get(ts).read();

        var filename = value.getFilename();

        if (badFile != null && !Objects.equals(filename, badFile)) {
          break;
        }
        badFile = null;

        if (Objects.equals(filename, lastFilename)) {
          log.info("Ignoring duplicate of file {}", lastFilename);
          previousFiles.remove(ts);
          continue;
        }

        var isLateFile = firstTs != null && ts.isBefore(firstTs);
        if (isLateFile) {
          log.info("Ignoring late file {}", lastFilename);
          previousFiles.remove(ts);
          continue;
        }

        log.info("Validating file {}", filename);

        var validRecordFile =
            value.getCache().stream()
                .filter(
                    recordFile -> {
                      var previousHash = recordFile.getPreviousHash();
                      var guard = lastHash.get().equals(previousHash);
                      if (!guard) {
                        log.debug(
                            "Previous hash {} in file {} doesn't match previous hash {}, seeking next candidate...",
                            previousHash,
                            filename,
                            lastHash.get());
                      }

                      return guard;
                    })
                .findAny();

        if (validRecordFile.isEmpty()) {
          log.warn("No candidates for file {} matching previous hash {}", filename, lastHash.get());
          badFile = filename;
        } else {
          var recordFile = validRecordFile.get();

          if (firstTs == null) {
            firstTs = ts;
          }
          lastTs = ts;
          lastFilename = filename;

          lastHash.set(recordFile.getHash());
          log.info("Emitting file {}", lastFilename);
          context.outputWithTimestamp(recordFile, ts);
          emittedElements++;
          previousFiles.remove(ts);
        }
      }

      if (firstTs != null && lastTs != null) {
        log.info("Emitted record files between {} - {}", firstTs, lastTs);
      }
      lastTsState.write(lastTs);
      lastHashState.write(lastHash.get());
      lastFilenameState.write(lastFilename);
      timerSet.write(false);
      log.info("Finishing validation/emission round: {}. Last filename: {}", context.timestamp(), lastFilename);
    }
  }
}
