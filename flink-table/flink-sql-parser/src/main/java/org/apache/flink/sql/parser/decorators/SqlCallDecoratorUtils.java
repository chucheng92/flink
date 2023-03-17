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

package org.apache.flink.sql.parser.decorators;

import org.apache.calcite.sql.SqlCall;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Operation decorator Utils. */
public class SqlCallDecoratorUtils {

    /**
     * Find all decorators on top of the underlying pure sql call.
     *
     * @param sqlCall original sql call, it may be decorated with various decorators.
     * @return all decorators on top of the underlying pure sql call.
     */
    public static List<SqlCallDecorator> findSqlCallDecorators(SqlCall sqlCall) {
        if (sqlCall == null) {
            return Collections.emptyList();
        }
        List<SqlCallDecorator> decorators = new ArrayList<>();
        while (sqlCall instanceof SqlCallDecorator) {
            decorators.add((SqlCallDecorator) sqlCall);
            sqlCall = ((SqlCallDecorator) sqlCall).getSqlCall();
        }

        return decorators;
    }

    /**
     * Take-off the decorators of the underlying final sql call and return it.
     *
     * @param sqlCall original sql call, it may be decorated with various decorators.
     * @return the underlying final sql call without any decorators.
     */
    public static SqlCall unwrapSqlCallDecorators(SqlCall sqlCall) {
        if (!(sqlCall instanceof SqlCallDecorator)) {
            return sqlCall;
        }
        while (sqlCall instanceof SqlCallDecorator) {
            sqlCall = ((SqlCallDecorator) sqlCall).getSqlCall();
        }

        return sqlCall;
    }

    private SqlCallDecoratorUtils() {}
}
