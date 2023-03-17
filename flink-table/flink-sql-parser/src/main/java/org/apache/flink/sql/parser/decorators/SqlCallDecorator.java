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
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nonnull;

/**
 * Abstract decorator class for sql calls which offer various different abilities. With call
 * decorator, we don't need to modify the original sql call classes, and they will automatically get
 * the new capabilities. Even, multiple decorators can be used to decorate sql calls.
 */
public abstract class SqlCallDecorator extends SqlCall {

    protected final SqlCall sqlCall;

    public SqlCallDecorator(SqlParserPos pos, @Nonnull SqlCall sqlCall) {
        super(pos);
        this.sqlCall = sqlCall;
    }

    public SqlCall getSqlCall() {
        return sqlCall;
    }
}
