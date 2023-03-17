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

import org.apache.flink.sql.parser.SqlLikeType;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/** Like decorator offer like filter ability to sql calls. */
public class LikeSqlCallDecorator extends SqlCallDecorator {

    // different like type such as like, ilike
    private final SqlLikeType likeType;
    private final boolean notLike;
    private final SqlCharStringLiteral likeLiteral;

    public LikeSqlCallDecorator(
            SqlCall sqlCall, String likeType, boolean notLike, SqlCharStringLiteral likeLiteral) {
        super(sqlCall.getParserPosition(), sqlCall);
        this.likeType = SqlLikeType.of(likeType);
        this.likeLiteral = requireNonNull(likeLiteral, "Like pattern must not be null");
        this.notLike = notLike;
    }

    public SqlLikeType getLikeType() {
        return likeType;
    }

    public boolean isLike() {
        return likeType == SqlLikeType.LIKE;
    }

    public boolean isILike() {
        return likeType == SqlLikeType.ILIKE;
    }

    public boolean isNotLike() {
        return notLike;
    }

    public SqlCharStringLiteral getLikeLiteral() {
        return likeLiteral;
    }

    public String getLikeSqlPattern() {
        return Objects.isNull(likeLiteral) ? null : likeLiteral.getValueAs(String.class);
    }

    @Override
    public SqlOperator getOperator() {
        return sqlCall.getOperator();
    }

    @Override
    public List<SqlNode> getOperandList() {
        return sqlCall.getOperandList();
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        sqlCall.unparse(writer, leftPrec, rightPrec);
        if (isNotLike()) {
            writer.keyword(String.format("NOT %s '%s'", getLikeType().name(), getLikeSqlPattern()));
        } else {
            writer.keyword(String.format("%s '%s'", getLikeType().name(), getLikeSqlPattern()));
        }
    }
}
