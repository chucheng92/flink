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

package org.apache.flink.sql.parser.dql;

import org.apache.flink.sql.parser.SqlLikeType;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/** Abstract show sql call supports filter with like. */
public abstract class SupportsShowLikeSqlCall extends SqlCall {

    // different like type such as like, ilike
    protected final SqlLikeType likeType;
    protected final boolean notLike;
    protected final SqlCharStringLiteral likeLiteral;

    protected SupportsShowLikeSqlCall(SqlParserPos pos) {
        super(pos);
        this.likeType = null;
        this.notLike = false;
        this.likeLiteral = null;
    }

    protected SupportsShowLikeSqlCall(
            SqlParserPos pos, String likeType, boolean notLike, SqlCharStringLiteral likeLiteral) {
        super(pos);
        if (likeType != null) {
            this.likeType = SqlLikeType.of(likeType);
            this.likeLiteral = requireNonNull(likeLiteral, "Like pattern must not be null");
        } else {
            this.likeType = null;
            this.likeLiteral = null;
        }
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

    public boolean isWithLike() {
        return isLike() || isILike();
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
}
