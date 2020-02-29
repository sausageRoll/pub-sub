package com.pubsub.broker;

import com.pubsub.model.Message;
import java.util.Collection;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class InMemoryMessageBroker implements MessageBroker {

    private final Map<String, Integer> lastRead = new ConcurrentHashMap<>();

    private final Map<String, BlockingQueue<Message<String>>> topics = new ConcurrentHashMap<>();

    @Override
    public boolean createTopic(String topicName) {
        return topics.putIfAbsent(topicName, new LinkedBlockingQueue<>()) == null;
    }

    @Override
    public String subscribe(String topic) {
        String subscriberKey = UUID.randomUUID().toString();
        lastRead.put(topic + subscriberKey, 0);
        return subscriberKey;
    }

    @Override
    public boolean unsubscribe(String topic, String subscriberKey) {
        return lastRead.remove(topic + subscriberKey) != null;
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

        Integer state = lastRead.get(topic + subscriberKey);
        if (state == 2) {
            try {
                return messages.poll(timeout, unit);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        } else if (state == 1) {
            lastRead.computeIfPresent(topic + subscriberKey, (t, o) -> 2);
            return pollTwice(messages);
        } else if (state == 0) {
            try {
                Message<String> message = messages.element();
                lastRead.computeIfPresent(topic + subscriberKey, (t, o) -> 1);
                return message;
            } catch (NoSuchElementException ex) {
                return null;
            }
        } else {
            throw new IllegalStateException("state should be 0 or 1 or 2");
        }
    }

    private synchronized Message<String> pollTwice(BlockingQueue<Message<String>> queue) {
        try {
            queue.poll(0, TimeUnit.MILLISECONDS);
            return queue.poll(0, TimeUnit.MILLISECONDS);
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
