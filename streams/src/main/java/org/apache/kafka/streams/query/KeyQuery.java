package org.apache.kafka.streams.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class KeyQuery<K, V> implements Query<V, KeyQuery.KeyQueryResult<V>> {

    public static class KeyQueryResult<V> implements QueryResult<V> {

        private Optional<V> results;
        private Optional<Exception> error;
        private List<String> executionInfo = new ArrayList<>();

        private KeyQueryResult(Optional<V> results,
                              Optional<Exception> error) {

            this.results = results;
            this.error = error;
        }

        @Override
        public Optional<V> results() {
            return results;
        }

        @Override
        public Optional<Exception> error() {
            return error;
        }

        public KeyQueryResult<V> withExecutionInfo(final String executionInfoLine) {
            executionInfo.add(executionInfoLine);
            return this;
        }

        @Override
        public List<String> executionInfo() {
            return executionInfo;
        }
    }
    private final K key;

    public KeyQuery() {
        this(null);
    }

    private KeyQuery(final K key) {
        this.key = key;
    }

    public KeyQuery<K, V> key(final K key) {
        final K nonNull = requireNonNull(key);
        return new KeyQuery<>(nonNull);
    }

    public K key() {
        return requireNonNull(key, "key wasn't set.");
    }

    @Override
    public KeyQueryResult<V> constructResult(Optional<V> value, Optional<Exception> error) {
        return new KeyQueryResult<>(value, error);
    }
}
