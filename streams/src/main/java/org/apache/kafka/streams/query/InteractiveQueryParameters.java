/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.query;

import static java.util.Objects.requireNonNull;

public class InteractiveQueryParameters<V, R extends QueryResult<V>, Q extends Query<V, R>> {
    private final String storeName;
    private final int partition;
    private final Q query;

    public InteractiveQueryParameters() {
        this(null, -1, null);
    }

    private InteractiveQueryParameters(final String storeName,
                                       final int partition,
                                       final Q query) {
        this.storeName = storeName;
        this.partition = partition;
        this.query = query;
    }

    public InteractiveQueryParameters<V, R, Q> storeName(final String storeName) {
        final String nonNull = requireNonNull(storeName);
        return new InteractiveQueryParameters<>(nonNull, partition, query);
    }

    public String storeName() {
        return requireNonNull(storeName, "storeName was not set.");
    }

    public InteractiveQueryParameters<V, R, Q> partition(final int partition) {
        final int nonNegative = requireNonNegative(partition, "partition must be non-negative.");
        return new InteractiveQueryParameters<>(storeName, nonNegative, query);
    }

    public int partition() {
        return requireNonNegative(partition, "partition was not set.");
    }

    public <V1, R1 extends QueryResult<V1>, Q1 extends Query<V1, R1>> InteractiveQueryParameters<V1, R1, Q1> query(final Q1 query) {
        final Q1 nonNull = requireNonNull(query);
        return new InteractiveQueryParameters<>(storeName, partition, nonNull);
    }

    public Q query() {
        return requireNonNull(query, "query was not set.");
    }

    private static int requireNonNegative(final int number, final String message) {
        if (number >= 0) {
            return number;
        } else {
            throw new IllegalArgumentException(message);
        }
    }
}
