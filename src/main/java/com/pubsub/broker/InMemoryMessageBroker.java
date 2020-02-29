package com.pubsub.broker;

import com.pubsub.model.Message;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class InMemoryMessageBroker implements MessageBroker {

    private final Map<String, BlockingQueue<Message<String>>> topics = new ConcurrentHashMap<>();

    @Override
    public boolean createTopic(String topicName) {
        return topics.putIfAbsent(topicName, new LinkedBlockingQueue<>()) == null;
    }

    @Override
    public String subscribe(String topic) {
        return UUID.randomUUID().toString();
    }

    @Override
    public void publishMessage(String topic, Message<String> message) {
        System.out.println(String.format("message %s came to topic %s", message.getValue(), topic));
        topics.computeIfPresent(topic, (t, q) -> {
            q.add(message);
            return q;
        });
    }

    @Override
    public Message<String> poll(String topic, String subscriberKey) {
        return poll(topic, subscriberKey, 0, TimeUnit.MILLISECONDS);
    }

    @Override
    public Message<String> poll(String topic, String subscriberKey, int timeout, TimeUnit unit) {
        BlockingQueue<Message<String>> messages = topics.get(topic);

        try {
            return messages.poll(timeout, unit);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int size() {
        return topics.values().stream()
                .mapToInt(Collection::size)
                .sum();
    }
}
