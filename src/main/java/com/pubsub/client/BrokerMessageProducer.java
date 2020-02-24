package com.pubsub.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pubsub.broker.MessageBroker;
import com.pubsub.model.Message;

public class BrokerMessageProducer {

    private final MessageBroker messageBroker;

    private final ObjectMapper objectMapper;

    public BrokerMessageProducer(MessageBroker messageBroker, ObjectMapper objectMapper) {
        this.messageBroker = messageBroker;
        this.objectMapper = objectMapper;
    }

    public void send(String topic, User user) {
        try {
            String message = objectMapper.writeValueAsString(user);
            messageBroker.publishMessage(topic, new Message<>(message));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }
}
