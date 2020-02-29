package com.pubsub.client;

import com.pubsub.broker.MessageBroker;
import com.pubsub.model.Message;
import java.util.concurrent.TimeUnit;

public class MessageConsumer {

    private final MessageBroker memoryMessageBroker;

    private final String topic;

    private String subscriberKey;

    public MessageConsumer(
            MessageBroker memoryMessageBroker, String topic
    ) {
        this.memoryMessageBroker = memoryMessageBroker;
        this.subscriberKey = memoryMessageBroker.subscribe(topic);
        this.topic = topic;
    }

    public Message<String> consume(int timeout, TimeUnit unit) {
        Message<String> poll = memoryMessageBroker.poll(topic, subscriberKey, timeout, unit);
        if (poll != null) {
            System.out.println(String.format(
                    "consumed message %s from topic %s with key %s",
                    poll.getValue(), topic, subscriberKey
            ));
        } else {
            System.out.println(String.format("topic %s is empty", topic));
        }
        return poll;
    }

    public void restart() {
        this.subscriberKey = memoryMessageBroker.subscribe(topic);
    }
}
