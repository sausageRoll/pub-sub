package com.pubsub.client;

import com.pubsub.broker.MessageBroker;
import com.pubsub.model.Message;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class MessageConsumer implements Iterator, Iterable {

    private final MessageBroker memoryMessageBroker;
    private final String topic;
    private final String subscriberKey;
    private final int timeout;
    private final TimeUnit unit;

    public MessageConsumer(
            MessageBroker memoryMessageBroker, String topic,
            int timeout, TimeUnit unit) {
        this.memoryMessageBroker = memoryMessageBroker;
        this.timeout = timeout;
        this.unit = unit;
        this.subscriberKey = memoryMessageBroker.subscribe(topic);
        this.topic = topic;
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public Object next() {
        return memoryMessageBroker.poll(topic, subscriberKey, timeout, unit);
    }

    @Override
    public Iterator iterator() {
        return this;
    }
}
