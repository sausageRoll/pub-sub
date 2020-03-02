package com.pubsub.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pubsub.broker.MessageBroker;

public class BrokerMessageProducer {

    private final MessageBroker messageBroker;

    public BrokerMessageProducer(MessageBroker messageBroker) {
        this.messageBroker = messageBroker;
    }

    public void send(String topic, Object message) {
        messageBroker.publishMessage(topic, message);
    }
}
