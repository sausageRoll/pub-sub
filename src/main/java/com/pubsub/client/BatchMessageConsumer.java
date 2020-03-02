package com.pubsub.client;

import com.pubsub.broker.MessageBroker;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class BatchMessageConsumer {

    private final MessageBroker memoryMessageBroker;

    private final String topic;

    private final String subscriberKey;

    private final int timeout;

    private final TimeUnit unit;

    private final int n;

    private final Iterator iterator;

    private final Iterable iterable;

    public <T> Iterable<T> iterable() {
        return iterable;
    }

    public BatchMessageConsumer(
            MessageBroker memoryMessageBroker, String topic,
            int timeout, TimeUnit unit, int n) {
        this.memoryMessageBroker = memoryMessageBroker;
        this.timeout = timeout;
        this.unit = unit;
        this.n = n;
        this.subscriberKey = memoryMessageBroker.subscribe(topic);
        this.topic = topic;
        iterator = new Iterator() {

            @Override
            public boolean hasNext() {
                return true;
            }

            @Override
            public Object next() {
                return memoryMessageBroker.poll(topic, subscriberKey, timeout, unit, n);
            }
        };
        iterable = () -> iterator;
    }
}
