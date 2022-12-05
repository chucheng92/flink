/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.hybrid.table;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.connector.base.source.hybrid.HybridSourceSplitEnumerator;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.util.List;

/**
 * A {@link ScanTableSource} that connect several numbers child sources to be a mixed hybrid source.
 * See {@link HybridSource}.
 */
public class HybridTableSource implements ScanTableSource {

    private final String tableName;
    private final ResolvedSchema tableSchema;
    private final List<Source<RowData, ?, ?>> childSources;
    private final Configuration configuration;

    public HybridTableSource(
            String tableName,
            @Nonnull List<Source<RowData, ?, ?>> childSources,
            Configuration configuration,
            ResolvedSchema tableSchema) {
        this.tableName = tableName;
        this.tableSchema = tableSchema;
        this.childSources = childSources;
        this.configuration = configuration;
    }

    @Override
    public DynamicTableSource copy() {
        return new HybridTableSource(tableName, childSources, configuration, tableSchema);
    }

    @Override
    public String asSummaryString() {
        return "HybridTableSource";
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        Preconditions.checkArgument(childSources.size() > 0);

        HybridSource<RowData> hybridSource;
        if (configuration.getBoolean(
                HybridConnectorOptions.OPTIONAL_SWITCHED_START_POSITION_ENABLED)) {
            HybridSource.HybridSourceBuilder<RowData, SplitEnumerator> builder =
                    HybridSource.builder(childSources.get(0));
            for (int i = 1; i < childSources.size(); i++) {
                final int sourceIndex = i;
                Boundedness boundedness = childSources.get(sourceIndex).getBoundedness();
                builder.addSource(
                        switchContext -> {
                            SplitEnumerator previousEnumerator =
                                    switchContext.getPreviousEnumerator();
                            // how to pass to kafka or other connector ? Can we add a method in new
                            // source api like startTimestamp();
                            // long switchedTimestamp = previousEnumerator.getEndTimestamp();
                            return childSources.get(sourceIndex);
                        },
                        boundedness);
            }
            hybridSource = builder.build();
        } else {
            HybridSource.HybridSourceBuilder<RowData, HybridSourceSplitEnumerator> builder =
                    HybridSource.builder(childSources.get(0));
            for (int i = 1; i < childSources.size(); i++) {
                builder.addSource(childSources.get(i));
            }
            hybridSource = builder.build();
        }
        return SourceProvider.of(hybridSource);
    }
}
