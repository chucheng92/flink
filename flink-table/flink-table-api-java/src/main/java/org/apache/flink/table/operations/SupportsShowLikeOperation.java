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

package org.apache.flink.table.operations;

import org.apache.flink.table.operations.utils.OperationLikeType;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Abstract show operation supports filter with like. */
public abstract class SupportsShowLikeOperation implements ShowOperation {

    // different like type such as like, ilike
    private final OperationLikeType likeType;
    private final boolean notLike;
    private final String likePattern;

    /** Use when there is no sub-clause. */
    protected SupportsShowLikeOperation() {
        this.likeType = null;
        this.notLike = false;
        this.likePattern = null;
    }

    /** Use when there is like. */
    protected SupportsShowLikeOperation(String likeType, boolean notLike, String likePattern) {
        if (likeType != null) {
            this.likeType = OperationLikeType.of(likeType);
            this.likePattern = checkNotNull(likePattern, "Like pattern must not be null");
        } else {
            this.likeType = null;
            this.likePattern = null;
        }
        this.notLike = notLike;
    }

    public OperationLikeType getLikeType() {
        return likeType;
    }

    public boolean isLike() {
        return likeType == OperationLikeType.LIKE;
    }

    public boolean isIlike() {
        return likeType == OperationLikeType.ILIKE;
    }

    public boolean isWithLike() {
        return isLike() || isIlike();
    }

    public boolean isNotLike() {
        return notLike;
    }

    public String getLikePattern() {
        return likePattern;
    }
}
