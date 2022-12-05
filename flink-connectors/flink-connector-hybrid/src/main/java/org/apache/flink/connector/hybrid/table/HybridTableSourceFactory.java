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

import org.apache.flink.api.connector.source.Source;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;

/**
 * Factory for creating {@link HybridTableSource} based-on FLIP-27 source API. It means that all
 * hybrid child source must be {@link Source}. Note some limitations: 1.hybrid source must specify
 * at least 2 child sources. 2.hybrid source works in batch mode then all child sources must be
 * bounded. 3.the first child source must be bounded (otherwise first child source never end).
 */
public class HybridTableSourceFactory implements DynamicTableSourceFactory {

    private static final Logger LOG = LoggerFactory.getLogger(HybridTableSourceFactory.class);
    public static final String IDENTIFIER = "hybrid";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final Configuration tableOptions = (Configuration) helper.getOptions();

        String tableName = context.getObjectIdentifier().toString();
        ResolvedCatalogTable hybridCatalogTable = context.getCatalogTable();
        ResolvedSchema tableSchema = hybridCatalogTable.getResolvedSchema();

        // validate params
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        for (String optionKey : tableOptions.toMap().keySet()) {
            if (optionKey.matches(HybridConnectorOptions.SOURCE_OPTION_REGEX)) {
                ConfigOption<String> childOption = key(optionKey).stringType().noDefaultValue();
                optionalOptions.add(childOption);
            }
        }
        optionalOptions.addAll(optionalOptions());
        FactoryUtil.validateFactoryOptions(requiredOptions(), optionalOptions, tableOptions);

        Set<String> consumedOptionKeys = new HashSet<>();
        consumedOptionKeys.add(CONNECTOR.key());
        consumedOptionKeys.add(HybridConnectorOptions.SOURCES.key());
        optionalOptions.stream().map(ConfigOption::key).forEach(consumedOptionKeys::add);
        FactoryUtil.validateUnconsumedKeys(
                factoryIdentifier(), tableOptions.keySet(), consumedOptionKeys);

        // process source option
        String sourceStr = tableOptions.get(HybridConnectorOptions.SOURCES);
        List<String> sourceList = Arrays.asList(sourceStr.split(","));
        if (sourceList.size() < 2) {
            throw new TableException(
                    String.format(
                            "Hybrid source '%s' option must specify at least 2 sources.",
                            HybridConnectorOptions.SOURCES.key()));
        }

        // parse schema-field-mappings
        ObjectMapper mapper = JacksonMapperFactory.createObjectMapper();
        String fieldMappingStr =
                tableOptions.get(HybridConnectorOptions.OPTIONAL_SCHEMA_FIELD_MAPPINGS);
        List<Map<String, String>> fieldMappingsList = null;
        if (!StringUtils.isEmpty(fieldMappingStr)) {
            try {
                fieldMappingsList =
                        mapper.readValue(
                                fieldMappingStr, new TypeReference<List<Map<String, String>>>() {});
            } catch (Exception e) {
                throw new TableException(
                        String.format(
                                "Failed to parse hybrid source '%s' option.",
                                HybridConnectorOptions.OPTIONAL_SCHEMA_FIELD_MAPPINGS.key()),
                        e);
            }
            if (fieldMappingsList.size() != sourceList.size()) {
                throw new IllegalArgumentException(
                        String.format(
                                "Hybrid source '%s' option split nums must equals child source nums.",
                                HybridConnectorOptions.OPTIONAL_SCHEMA_FIELD_MAPPINGS.key()));
            }
        }

        LOG.info("Hybrid source is consisted of {}.", sourceList);

        // generate concrete child sources & concat sources to final hybrid source
        List<Source<RowData, ?, ?>> childSources = new ArrayList<>();
        ClassLoader cl = HybridTableSourceFactory.class.getClassLoader();
        for (int i = 0; i < sourceList.size(); i++) {
            ResolvedCatalogTable childCatalogTable;
            ResolvedSchema childResolvedSchema = null;
            // override schema
            if (fieldMappingsList != null) {
                Map<String, String> fieldMappings = fieldMappingsList.get(i);
                childResolvedSchema =
                        createChildSchemaUsingFieldMappings(tableSchema, fieldMappings);
            }
            if (childResolvedSchema != null) {
                childCatalogTable =
                        new ResolvedCatalogTable(
                                hybridCatalogTable
                                        .getOrigin()
                                        .copy(
                                                extractChildSourceOptions(
                                                        hybridCatalogTable.getOptions(), i)),
                                childResolvedSchema);
            } else {
                childCatalogTable =
                        hybridCatalogTable.copy(
                                extractChildSourceOptions(hybridCatalogTable.getOptions(), i));
            }

            DynamicTableSource tableSource =
                    FactoryUtil.createDynamicTableSource(
                            null,
                            context.getObjectIdentifier(),
                            childCatalogTable,
                            Collections.emptyMap(),
                            context.getConfiguration(),
                            cl,
                            true);
            if (tableSource instanceof ScanTableSource) {
                ScanTableSource.ScanRuntimeProvider provider =
                        ((ScanTableSource) tableSource)
                                .getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
                if (provider instanceof SourceProvider) {
                    Source<RowData, ?, ?> source = ((SourceProvider) provider).createSource();
                    childSources.add(source);
                } else {
                    throw new UnsupportedOperationException(
                            provider.getClass().getCanonicalName() + " is unsupported now.");
                }
            } else {
                // hybrid source only support ScanTableSource
                throw new TableException(
                        String.format(
                                "%s is not a ScanTableSource, please check it.",
                                tableSource.getClass().getName()));
            }
        }
        LOG.info("Generate hybrid child sources with: {}.", childSources);

        // post check
        Preconditions.checkArgument(sourceList.size() == childSources.size());

        return new HybridTableSource(tableName, childSources, tableOptions, tableSchema);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(HybridConnectorOptions.SOURCES);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(HybridConnectorOptions.OPTIONAL_SCHEMA_FIELD_MAPPINGS);
        return optionalOptions;
    }

    protected Map<String, String> extractChildSourceOptions(
            Map<String, String> originalOptions, int index) {
        if (originalOptions == null || originalOptions.isEmpty()) {
            return originalOptions;
        }
        String optionPrefix = index + HybridConnectorOptions.SOURCE_DELIMITER;
        Map<String, String> sourceOptions =
                originalOptions.entrySet().stream()
                        .filter(entry -> entry.getKey().startsWith(optionPrefix))
                        .collect(
                                Collectors.toMap(
                                        entry ->
                                                StringUtils.removeStart(
                                                        entry.getKey(), optionPrefix),
                                        Map.Entry::getValue));
        String[] sources = originalOptions.get(HybridConnectorOptions.SOURCES.key()).split(",");
        sourceOptions.put(FactoryUtil.CONNECTOR.key(), sources[index]);

        return sourceOptions;
    }

    protected ResolvedSchema createChildSchemaUsingFieldMappings(
            @Nonnull ResolvedSchema originalSchema, Map<String, String> fieldMappings) {
        if (fieldMappings == null || fieldMappings.isEmpty()) {
            return originalSchema;
        }
        List<Column> columns = originalSchema.getColumns();
        List<Column> newColumns = new ArrayList<>();
        for (Column column : columns) {
            String columnName = column.getName();
            if (fieldMappings.get(columnName) != null) {
                String newName = fieldMappings.get(columnName);
                Column newColumn;
                switch (column.getClass().getSimpleName()) {
                    case "PhysicalColumn":
                        newColumn =
                                Column.physical(newName, column.getDataType())
                                        .withComment(column.getComment().orElse(null));
                        break;
                    case "ComputedColumn":
                        Column.ComputedColumn computedColumn = (Column.ComputedColumn) column;
                        newColumn =
                                Column.computed(newName, computedColumn.getExpression())
                                        .withComment(column.getComment().orElse(null));
                        break;
                    case "MetadataColumn":
                        Column.MetadataColumn metaDataColumn = (Column.MetadataColumn) column;
                        newColumn =
                                Column.metadata(
                                                newName,
                                                metaDataColumn.getDataType(),
                                                metaDataColumn.getMetadataKey().orElse(null),
                                                metaDataColumn.isVirtual())
                                        .withComment(column.getComment().orElse(null));
                        break;
                    default:
                        throw new UnsupportedOperationException(
                                "Not supported column class type: " + column.getClass());
                }
                newColumns.add(newColumn);
            } else {
                newColumns.add(column);
            }
        }

        return new ResolvedSchema(
                newColumns,
                originalSchema.getWatermarkSpecs(),
                originalSchema.getPrimaryKey().orElse(null));
    }
}
