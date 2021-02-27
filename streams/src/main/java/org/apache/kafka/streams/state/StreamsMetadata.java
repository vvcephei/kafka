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
package org.apache.kafka.streams.state;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Represents the state of an instance (process) in a {@link KafkaStreams} application.
 * It contains the user supplied {@link HostInfo} that can be used by developers to build
 * APIs and services to connect to other instances, the Set of state stores available on
 * the instance and the Set of {@link TopicPartition}s available on the instance.
 * NOTE: This is a point in time view. It may change when rebalances happen.
 */
public class StreamsMetadata {
    /**
     * Sentinel to indicate that the StreamsMetadata is currently unavailable. This can occur during rebalance
     * operations.
     */
    public final static StreamsMetadata NOT_AVAILABLE = new StreamsMetadata(HostInfo.unavailable(),
                                                                            Collections.emptyMap(),
                                                                            Collections.emptySet(),
                                                                            Collections.emptyMap(),
                                                                            Collections.emptySet());

    private final HostInfo hostInfo;

    private final Map<String, Set<Integer>> activeStorePartitions;

    private final Map<String, Set<Integer>> standbyStorePartitions;

    private final Set<TopicPartition> topicPartitions;

    private final Set<TopicPartition> standbyTopicPartitions;

    public StreamsMetadata(final HostInfo hostInfo,
                           final Map<String, Set<Integer>> activeStorePartitions,
                           final Set<TopicPartition> topicPartitions,
                           final Map<String, Set<Integer>> standbyStorePartitions,
                           final Set<TopicPartition> standbyTopicPartitions) {

        this.hostInfo = hostInfo;
        this.topicPartitions = topicPartitions;
        this.standbyTopicPartitions = standbyTopicPartitions;

        this.activeStorePartitions = activeStorePartitions;
        this.standbyStorePartitions = standbyStorePartitions;
    }

    /**
     * The value of {@link org.apache.kafka.streams.StreamsConfig#APPLICATION_SERVER_CONFIG} configured for the streams
     * instance, which is typically host/port
     *
     * @return {@link HostInfo} corresponding to the streams instance
     */
    public HostInfo hostInfo() {
        return hostInfo;
    }

    /**
     * State stores owned by the instance as an active replica
     *
     * @return set of active state store names
     */
    public Set<String> stateStoreNames() {
        return Collections.unmodifiableSet(activeStorePartitions.keySet());
    }

    /**
     * Topic partitions consumed by the instance as an active replica
     *
     * @return set of active topic partitions
     */
    public Set<TopicPartition> topicPartitions() {
        return Collections.unmodifiableSet(topicPartitions);
    }

    /**
     * (Source) Topic partitions for which the instance acts as standby.
     *
     * @return set of standby topic partitions
     */
    public Set<TopicPartition> standbyTopicPartitions() {
        return Collections.unmodifiableSet(standbyTopicPartitions);
    }

    /**
     * State stores owned by the instance as a standby replica
     *
     * @return set of standby state store names
     */
    public Set<String> standbyStateStoreNames() {
        return Collections.unmodifiableSet(standbyStorePartitions.keySet());
    }

    public Map<String, Set<Integer>> activeStorePartitions() {
        return activeStorePartitions;
    }

    public Map<String, Set<Integer>> standbyStorePartitions() {
        return standbyStorePartitions;
    }

    public String host() {
        return hostInfo.host();
    }

    @SuppressWarnings("unused")
    public int port() {
        return hostInfo.port();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final StreamsMetadata that = (StreamsMetadata) o;
        return Objects.equals(hostInfo, that.hostInfo) && Objects.equals(activeStorePartitions, that.activeStorePartitions) && Objects.equals(standbyStorePartitions, that.standbyStorePartitions) && Objects.equals(topicPartitions, that.topicPartitions) && Objects.equals(standbyTopicPartitions, that.standbyTopicPartitions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hostInfo, activeStorePartitions, standbyStorePartitions, topicPartitions, standbyTopicPartitions);
    }

    @Override
    public String toString() {
        return "StreamsMetadata{" +
               "hostInfo=" + hostInfo +
               ", activeStorePartitions=" + activeStorePartitions +
               ", standbyStorePartitions=" + standbyStorePartitions +
               ", topicPartitions=" + topicPartitions +
               ", standbyTopicPartitions=" + standbyTopicPartitions +
               '}';
    }
}
