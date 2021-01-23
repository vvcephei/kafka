package org.apache.kafka.jmh.streams.processor.internals;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.SourceNode;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class MockSourceNode<KIn, VIn, KOut, VOut> extends SourceNode<KIn, VIn, KOut, VOut> {

    private static final String NAME = "MOCK-SOURCE-";
    private static final AtomicInteger INDEX = new AtomicInteger(1);

    public int numReceived = 0;
    public final ArrayList<KIn> keys = new ArrayList<>();
    public final ArrayList<VIn> values = new ArrayList<>();
    public boolean initialized;
    public boolean closed;

    public MockSourceNode(final Deserializer<KIn> keyDeserializer, final Deserializer<VIn> valDeserializer) {
        super(NAME + INDEX.getAndIncrement(), keyDeserializer, valDeserializer);
    }

    @Override
    public void process(final Record<KIn, VIn> record) {
        numReceived++;
        keys.add(record.key());
        values.add(record.value());
    }

    @Override
    public void init(final InternalProcessorContext context) {
        super.init(context);
        initialized = true;
    }

    @Override
    public void close() {
        super.close();
        closed = true;
    }
}
