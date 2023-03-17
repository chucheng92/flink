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

package org.apache.flink.table.operations.decorators;

import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.utils.OperationLikeType;

import static java.util.Objects.requireNonNull;

/** Like decorator offer like filter ability to operations. */
public class LikeOperationDecorator extends OperationDecorator {

    // different like type such as like, ilike
    private final OperationLikeType likeType;
    private final boolean notLike;
    private final String likePattern;

    public LikeOperationDecorator(
            Operation operation, String likeType, boolean notLike, String likePattern) {
        super(operation);
        this.likeType = OperationLikeType.of(likeType);
        this.likePattern = requireNonNull(likePattern, "Like pattern must not be null");
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

    public boolean isNotLike() {
        return notLike;
    }

    public String getLikePattern() {
        return likePattern;
    }

    @Override
    public String asSummaryString() {
        StringBuilder builder = new StringBuilder(operation.asSummaryString());
        if (isNotLike()) {
            builder.append(String.format(" NOT %s '%s'", getLikeType().name(), getLikePattern()));
        } else {
            builder.append(String.format(" %s '%s'", getLikeType().name(), getLikePattern()));
        }
        return builder.toString();
    }
}
