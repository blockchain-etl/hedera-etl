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

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import lombok.RequiredArgsConstructor;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.extensions.gcp.auth.CredentialFactory;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

public class HederaETLApplication {

  public static void main(String[] args) throws IOException {
    var applicationOptions =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .withoutStrictParsing()
            .as(ApplicationOptions.class);

    applicationOptions.setCredentialFactoryClass(DefaultCredentialsWithQuotaProjectFactory.class);

    switch (applicationOptions.getMode()) {
      case BATCH:
        var batchOptions = applicationOptions.as(BatchStorageToBigQueryPipelineOptions.class);
        new BatchStorageToBigQueryPipeline(batchOptions).run();
        break;
      case STREAMING:
        var streamingOptions =
            applicationOptions.as(StreamingStorageToBigQueryPipelineOptions.class);
        new StreamingStorageToBigQueryPipeline(streamingOptions).run();
        break;
      default:
        throw new UnsupportedOperationException("Unknown mode " + applicationOptions.getMode());
    }
  }

  public interface ApplicationOptions
      extends BatchStorageToBigQueryPipelineOptions,
          StreamingStorageToBigQueryPipelineOptions,
          DataflowPipelineOptions,
          GcpOptions,
          PipelineOptions {
    @Description("Which mode to use")
    @Default.Enum("BATCH")
    // TODO: remove default value after implementation of realtime pipeline
    Mode getMode();

    void setMode(Mode value);

    enum Mode {
      BATCH,
      STREAMING,
    }
  }

  // Workaround for Quota Project ID missing from Compute default service account,
  // which prohibits using resources with Requester Pays mode enabled
  @RequiredArgsConstructor
  public static class DefaultCredentialsWithQuotaProjectFactory implements CredentialFactory {

    private final String project;

    public static CredentialFactory fromOptions(PipelineOptions options) {
      return new DefaultCredentialsWithQuotaProjectFactory(
          options.as(GcpOptions.class).getProject());
    }

    @Override
    public @Nullable @UnknownKeyFor @Initialized Credentials getCredential()
        throws IOException, GeneralSecurityException {
      List<String> SCOPES =
          List.of(
              "https://www.googleapis.com/auth/cloud-platform",
              "https://www.googleapis.com/auth/devstorage.full_control",
              "https://www.googleapis.com/auth/userinfo.email",
              "https://www.googleapis.com/auth/datastore",
              "https://www.googleapis.com/auth/pubsub");

      return GoogleCredentials.getApplicationDefault()
          .createScoped(SCOPES)
          .createWithQuotaProject(project);
    }
  }
}
