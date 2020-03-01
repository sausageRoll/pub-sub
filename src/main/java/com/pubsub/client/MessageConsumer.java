package com.pubsub.client;

import com.pubsub.broker.MessageBroker;
import com.pubsub.model.Message;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class MessageConsumer {

    private final MessageBroker memoryMessageBroker;
    private final String topic;
    private final String subscriberKey;
    private final int timeout;
    private final TimeUnit unit;
    private final Iterator iterator;
    private final Iterable iterable;

    public <T> Iterable<T> iterable() {
        return iterable;
    }

    public MessageConsumer(
            MessageBroker memoryMessageBroker, String topic,
            int timeout, TimeUnit unit) {
        this.memoryMessageBroker = memoryMessageBroker;
        this.timeout = timeout;
        this.unit = unit;
        this.subscriberKey = memoryMessageBroker.subscribe(topic);
        this.topic = topic;
        iterator = new Iterator() {

            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public Object next() {
                return memoryMessageBroker.poll(topic, subscriberKey, timeout, unit);
            }
        };

        iterable = new Iterable() {

            @Override
            public Iterator iterator() {
                return iterator;
            }
        };
    }
}
