package org.apache.kafka.jmh.streams.processor.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.errors.LogAndFailExceptionHandler;
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp;
import org.apache.kafka.streams.processor.internals.PartitionGroup;
import org.apache.kafka.streams.processor.internals.RecordQueue;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class PartitionGroupBenchmark {

    @Param({"0", "1"})
    private int recordCount;

    @Param({"-1", "0", "1"})
    private int lag;

    PartitionGroup partitionGroup;

    @Setup(Level.Trial)
    public void setup() {
        final Metrics metrics = new Metrics(new MockTime());

        Map<TopicPartition, RecordQueue> partitionQueues = new HashMap<>();
        final TopicPartition topicPartition = new TopicPartition("table-in", 0);
        final ByteArrayDeserializer keyDeserializer = new ByteArrayDeserializer();
        final ByteArrayDeserializer valDeserializer = new ByteArrayDeserializer();
        final RecordQueue recordQueue = new RecordQueue(
            topicPartition,
            new MockSourceNode<byte[], byte[], byte[], byte[]>(keyDeserializer, valDeserializer),
            new FailOnInvalidTimestamp(),
            new LogAndFailExceptionHandler(),
            new NoOpProcessorContext(),
            new LogContext()
        );
        partitionQueues.put(topicPartition, recordQueue);
        partitionGroup = new PartitionGroup(
            new LogContext(),
            partitionQueues,
            metrics.sensor("recordLateness"),
            metrics.sensor("enforcedProcessing"),
            0
        );

        final ArrayList<ConsumerRecord<byte[], byte[]>> consumerRecords = new ArrayList<>(recordCount);
        for (int i = 0; i < recordCount; i++) {
            final ConsumerRecord<byte[], byte[]> consumerRecord = new ConsumerRecord<>(
                topicPartition.topic(),
                topicPartition.partition(),
                i,
                0L,
                TimestampType.CREATE_TIME,
                0L,
                0,
                0,
                new byte[0],
                new byte[0]
            );
            consumerRecords.add(i, consumerRecord);
        }
        partitionGroup.addRawRecords(
            topicPartition,
            consumerRecords
        );

        if (lag >= 0) {
            partitionGroup.addFetchedMetadata(
                topicPartition,
                new ConsumerRecords.Metadata(0L, 0L, (long) lag)
            );
        }
    }

    @Benchmark
    public boolean isProcessable() {
        return partitionGroup.readyToProcess(0L);
    }

    public static void main(String[] args) {

        for (Integer rc : asList(0, 1)) {
            for (Integer l : asList(-1, 0, 1)) {
                final PartitionGroupBenchmark partitionGroupBenchmark = new PartitionGroupBenchmark();
                partitionGroupBenchmark.recordCount = rc;
                partitionGroupBenchmark.lag = l;
                partitionGroupBenchmark.setup();
                final boolean processable = partitionGroupBenchmark.isProcessable();
                System.out.printf("%d %d %s %n", rc, l, processable);
            }
        }
    }
}
