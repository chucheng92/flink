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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

/** Integration test for the {@link HybridTableSource}. */
public class HybridTableSourceITCase extends TestLogger {

    private TableEnvironment tEnv;

    private void initEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings.Builder streamBuilder =
                EnvironmentSettings.newInstance().inStreamingMode();
        EnvironmentSettings streamSettings = streamBuilder.build();
        this.tEnv = StreamTableEnvironment.create(env, streamSettings);
    }

    @Before
    public void before() {
        initEnv();
    }

    @Test
    public void testHybridSourceWithDDLFromCsvData() throws Exception {
        tEnv.executeSql(
                "CREATE TABLE hybrid_source(\n"
                        + "  f0 VARCHAR,\n"
                        + "  f1 VARCHAR,\n"
                        + "  f2 BIGINT\n"
                        + ") WITH (\n"
                        + "  'connector' = 'hybrid',\n"
                        + "  'sources' = 'filesystem,filesystem',\n"
                        + "  '0.path' = '/Users/chucheng/TMP/a.csv',\n"
                        + "  '0.format' = 'csv',\n"
                        + "  '1.path' = '/Users/chucheng/TMP/b.csv',\n"
                        + "  '1.format' = 'csv',\n"
                        + "  '1.source.monitor-interval' = '3'\n"
                        + ")");
        tEnv.executeSql(
                "CREATE TABLE print_out (a varchar, b varchar, c bigint)\n"
                        + "    with ('connector' = 'print')");
        tEnv.executeSql("INSERT INTO print_out SELECT * FROM hybrid_source").await();
    }

    @Test
    public void testHybridSourceWithDDLFromJsonData() throws Exception {
        tEnv.executeSql(
                "CREATE TABLE hybrid_source(\n"
                        + "  name VARCHAR,\n"
                        + "  gender VARCHAR,\n"
                        + "  age BIGINT\n"
                        + ") WITH (\n"
                        + "  'connector' = 'hybrid',\n"
                        + "  'sources' = 'filesystem,filesystem',\n"
                        + "  'schema-field-mappings'='[{},{\"name\": \"uid\"}]',\n"
                        + "  '0.path' = '/Users/chucheng/TMP/a.json',\n"
                        + "  '0.format' = 'json',\n"
                        + "  '1.path' = '/Users/chucheng/TMP/b.json',\n"
                        + "  '1.format' = 'json'\n"
                        + ")");
        tEnv.executeSql(
                "CREATE TABLE print_out (a varchar, b varchar, c bigint)\n"
                        + "    with ('connector' = 'print')");
        tEnv.executeSql("INSERT INTO print_out SELECT * FROM hybrid_source").await();
    }

    @Test
    public void testHybridSource() throws Exception {
        tEnv.executeSql(
                "CREATE TABLE hybrid_source(\n"
                        + "  name VARCHAR,\n"
                        + "  gender VARCHAR,\n"
                        + "  age BIGINT\n"
                        + ") WITH (\n"
                        + "  'connector' = 'filesystem',\n"
                        + "  'path' = '/Users/chucheng/TMP/a.json',\n"
                        + "  'format' = 'json'\n"
                        + ")");
        tEnv.executeSql(
                "CREATE TABLE print_out (a varchar, b varchar, c bigint)\n"
                        + "    with ('connector' = 'print')");
        tEnv.executeSql("INSERT INTO print_out SELECT * FROM hybrid_source").await();
    }
}
