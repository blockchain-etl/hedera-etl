package com.hedera.etl;

/*-
 * ‌
 * Hedera ETL
 * ​
 * Copyright (C) 2020 Hedera Hashgraph, LLC
 * ​
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ‍
 */

import java.time.LocalDate;
import java.util.List;
import java.util.stream.Stream;

import com.google.common.collect.Streams;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

public interface BatchStorageToBigQueryPipelineOptions extends PipelineOptions {
  @Description("File with inputs")
  String getInputFileList();

  void setInputFileList(String value);

  @Description("GCS bucket with Record Files")
  String getInputBucket();

  void setInputBucket(String value);

  @Description("Node from which to ingest bytes")
  @Default.String("0.0.3")
  String getInputNode();

  void setInputNode(String value);

  @Description("List of nodes from which to ingest bytes")
  @Default.InstanceFactory(NodeListFactory.class)
  List<String> getInputNodes();

  void setInputNodes(List<String> value);

  @Description("Date from which ingest Record files")
  LocalDate getIngestionDate();

  void setIngestionDate(LocalDate value);

  @Description("Date to which ingest Record files")
  @Default.InstanceFactory(IngestionEndDateFactory.class)
  LocalDate getIngestionEndDate();

  void setIngestionEndDate(LocalDate value);

  @Description("Disable verification")
  @Default.Boolean(true)
  Boolean getEnableVerification();

  void setEnableVerification(Boolean value);

  @Description("Start above this file")
  @Default.String("")
  String getStartAboveFile();

  void setStartAboveFile(String value);

  @Description("Last hash")
  @Default.String(
      "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000")
  String getLastValidHash();

  void setLastValidHash(String value);

  @Description(
      "The file path to consume from. The name should be in the format of "
          + "filesystem://path/to/files")
  @Default.InstanceFactory(InputPatternFactory.class)
  List<String> getInputPathPatterns();

  void setInputPathPatterns(List<String> value);

  class InputPatternFactory implements DefaultValueFactory<List<String>> {
    public List<String> create(@NonNull PipelineOptions pipelineOptions) {
      var options = pipelineOptions.as(BatchStorageToBigQueryPipelineOptions.class);
      var endingDate =
          options.getIngestionEndDate() == null
              ? options.getIngestionDate()
              : options.getIngestionEndDate();

      var dates =
          Streams.concat(Stream.of(endingDate), options.getIngestionDate().datesUntil(endingDate))
              .toList();

      return options.getInputNodes().stream()
          .flatMap(
              node ->
                  dates.stream()
                      .map(
                          date ->
                              "gs://"
                                  + options.getInputBucket()
                                  + "/recordstreams/record"
                                  + node
                                  + "/"
                                  + date
                                  + "*"))
          .toList();
    }
  }

  @Description("Output dataset for restricted access dataset")
  String getRestrictedAccessDataset();

  void setRestrictedAccessDataset(String value);

  @Description("Output dataset for open access dataset")
  String getOpenAccessDataset();

  void setOpenAccessDataset(String value);

  @Description("Output dataset for technical dataset")
  String getTechnicalDataset();

  void setTechnicalDataset(String value);

  @Description("Disable merge history input")
  @Default.Boolean(false)
  boolean getDisableMergeHistoryInput();

  void setDisableMergeHistoryInput(boolean value);

  @Description(
      "List of enabled output entities, restricted.tablename enables tables in restricted dataset and open.tablename in the open one")
  @Default.InstanceFactory(EmptyListFactory.class)
  List<String> getEnabledOutputs();

  void setEnabledOutputs(List<String> value);

  class EmptyListFactory implements DefaultValueFactory<List<String>> {
    @Override
    public List<String> create(@UnknownKeyFor @NonNull @Initialized PipelineOptions options) {
      return List.of();
    }
  }

  class IngestionEndDateFactory implements DefaultValueFactory<LocalDate> {
    @Override
    public LocalDate create(@UnknownKeyFor @NonNull @Initialized PipelineOptions pipelineOptions) {
      var options = pipelineOptions.as(BatchStorageToBigQueryPipelineOptions.class);
      return options.getIngestionDate();
    }
  }

  class NodeListFactory implements DefaultValueFactory<List<String>> {
    @Override
    public List<String> create(@UnknownKeyFor @NonNull @Initialized PipelineOptions options) {
      return List.of("0.0.3");
    }
  }
}
