package com.pubsub.client;

import com.pubsub.broker.MessageBroker;
import com.pubsub.model.Message;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class BatchMessageConsumer implements Iterator<Iterable>,
        Iterable<Iterable> {

    private final MessageBroker memoryMessageBroker;

    private final String topic;

    private final String subscriberKey;

    private final int timeout;

    private final TimeUnit unit;

    private final int n;

    public BatchMessageConsumer(
            MessageBroker memoryMessageBroker, String topic,
            int timeout, TimeUnit unit, int n) {
        this.memoryMessageBroker = memoryMessageBroker;
        this.timeout = timeout;
        this.unit = unit;
        this.n = n;
        this.subscriberKey = memoryMessageBroker.subscribe(topic);
        this.topic = topic;
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public Iterable next() {
        return memoryMessageBroker.poll(topic, subscriberKey, timeout, unit, n);
    }

    @Override
    public Iterator<Iterable> iterator() {
        return this;
    }
}
