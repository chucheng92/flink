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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Operation decorator Utils. */
public class OperatorDecoratorUtils {

    /**
     * Find all decorators on top of the underlying operation.
     *
     * @param operation original operation, it may be decorated with various decorators.
     * @return all decorators on top of the underlying operation.
     */
    public static List<OperationDecorator> findOperationDecorators(Operation operation) {
        if (operation == null) {
            return Collections.emptyList();
        }
        List<OperationDecorator> decorators = new ArrayList<>();
        while (operation instanceof OperationDecorator) {
            decorators.add((OperationDecorator) operation);
            operation = ((OperationDecorator) operation).getOperation();
        }

        return decorators;
    }

    /**
     * Take-off the decorators of the underlying final operation and return it.
     *
     * @param operation original operation, it may be decorated with various decorators.
     * @return the underlying final operation without any decorators.
     */
    public static Operation unwrapOperationDecorators(Operation operation) {
        if (!(operation instanceof OperationDecorator)) {
            return operation;
        }
        while (operation instanceof OperationDecorator) {
            operation = ((OperationDecorator) operation).getOperation();
        }

        return operation;
    }

    private OperatorDecoratorUtils() {}
}
