package com.pubsub.broker;

import com.pubsub.model.Message;
import java.util.concurrent.TimeUnit;

public interface MessageBroker {

    boolean createTopic(String topicName);

    String subscribe(String topic);

    boolean unsubscribe(String topic, String key);

    <T> void publishMessage(String topic, T message);

    <T> T poll(String topic, String subscriberKey);

    <T> T poll(String topic, String subscriberKey, int timeout, TimeUnit unit);

    Iterable poll(String topic, String subscriberKey, int timeout, TimeUnit unit, int n);

    int size();
}
