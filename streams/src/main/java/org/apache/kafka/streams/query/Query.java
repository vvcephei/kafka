package org.apache.kafka.streams.query;

import java.util.Optional;

public interface Query<V, R extends QueryResult<V>> {
    default boolean cached() {
        return true;
    }

    R constructResult(Optional<V> value, Optional<Exception> error);
}
