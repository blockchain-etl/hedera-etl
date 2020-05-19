package com.hedera.dedupe;

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

import com.google.api.gax.core.CredentialsProvider;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.stackdriver.StackdriverConfig;
import io.micrometer.stackdriver.StackdriverMeterRegistry;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.gcp.core.GcpProjectIdProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableAsync
@Configuration
@RequiredArgsConstructor
public class DedupeConfiguration {

    private final GcpProjectIdProvider projectIdProvider;
    private final CredentialsProvider credentialsProvider;
    private final DedupeProperties properties;

    @Bean
    StackdriverConfig stackdriverConfig() {
        return new StackdriverConfig() {
            @Override
            public String projectId() {
                return projectIdProvider.getProjectId();
            }

            // Setting "management.metrics.export.stackdriver: false" is not working
            @Override
            public boolean enabled() {
                return properties.isMetricsEnabled();
            }

            @Override
            public String get(String key) {
                return null;
            }

            @Override
            public CredentialsProvider credentials() {
                return credentialsProvider;
            }
        };
    }

    @Bean
    MeterRegistry stackdriverMeterRegistry(StackdriverConfig stackdriverConfig) {
        return StackdriverMeterRegistry.builder(stackdriverConfig).build();
    }

    // Scheduling is disabled for testing
    @Configuration
    @EnableScheduling
    @ConditionalOnProperty(prefix = "hedera.dedupe.scheduling", name = "enabled",  matchIfMissing = true)
    protected static class SchedulingConfiguration {
    }
}
