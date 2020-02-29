package com.pubsub.broker;

import com.pubsub.model.Message;
import java.util.concurrent.TimeUnit;

public interface MessageBroker<T> {

    boolean createTopic(String topicName);

    String subscribe(String topic);

    boolean unsubscribe(String topic, String key);

    void publishMessage(String topic, T message);

    T poll(String topic, String subscriberKey);

    T poll(String topic, String subscriberKey, int timeout, TimeUnit unit);

    Iterable<T> poll(String topic, String subscriberKey, int timeout, TimeUnit unit, int n);

    int size();
}
