package com.pubsub.broker;

import com.pubsub.model.Message;
import java.util.concurrent.TimeUnit;

public interface MessageBroker {

    boolean createTopic(String topicName);

    String subscribe(String topic);

    boolean unsubscribe(String topic, String subscriberKey);

    void publishMessage(String topic, Message<String> message);

    Message<String> poll(String topic, String subscriberKey);

    Message<String> poll(String topic, String subscriberKey, int timeout, TimeUnit unit);

    int size();
}
