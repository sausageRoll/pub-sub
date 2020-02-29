package com.pubsub.client;

import com.pubsub.broker.MessageBroker;
import com.pubsub.model.Message;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class MessageConsumer<T> implements Iterator<T>, Iterable<T> {

    private final MessageBroker<T> memoryMessageBroker;
    private final String topic;
    private final String subscriberKey;
    private final int timeout;
    private final TimeUnit unit;

    public MessageConsumer(
            MessageBroker<T> memoryMessageBroker, String topic,
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
    public T next() {
        return memoryMessageBroker.poll(topic, subscriberKey, timeout, unit);
    }

    @Override
    public Iterator<T> iterator() {
        return this;
    }
}
