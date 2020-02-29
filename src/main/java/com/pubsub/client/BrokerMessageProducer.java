package com.pubsub.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pubsub.broker.MessageBroker;

public class BrokerMessageProducer<T> {

    private final MessageBroker<T> messageBroker;

    private final ObjectMapper objectMapper;

    public BrokerMessageProducer(MessageBroker<T> messageBroker, ObjectMapper objectMapper) {
        this.messageBroker = messageBroker;
        this.objectMapper = objectMapper;
    }

    public void send(String topic, T message) {
        messageBroker.publishMessage(topic, message);
    }
}
