package org.apache.kafka.streams.query;

import java.util.List;
import java.util.Optional;

public interface QueryResult<V> {
    Optional<V> results();
    Optional<Exception> error();
    List<String> executionInfo();
}
