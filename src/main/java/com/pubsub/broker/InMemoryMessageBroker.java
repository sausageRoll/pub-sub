package com.pubsub.broker;

import com.pubsub.model.Message;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class InMemoryMessageBroker<T> implements MessageBroker<T> {

    private final Map<String, String> topicToConsumer = new ConcurrentHashMap<>();
    private final Map<String, BlockingQueue<T>> topics = new ConcurrentHashMap<>();

    @Override
    public boolean createTopic(String topicName) {
        return topics.putIfAbsent(topicName, new LinkedBlockingQueue<>()) == null;
    }

    @Override
    public String subscribe(String topic) {
        if (topicToConsumer.get(topic) != null) {
            throw new IllegalStateException("topic is already subscribed");
        }
        return topicToConsumer.computeIfAbsent(topic, (t) -> UUID.randomUUID().toString());
    }

    @Override
    public boolean unsubscribe(String topic, String key) {
        return topicToConsumer.remove(topic, key);
    }

    @Override
    public void publishMessage(String topic, T message) {
        System.out.println(String.format("message %s came to topic %s", message.toString(), topic));
        topics.compute(topic, (t, q) -> {
            if (q != null) {
                q.add(message);
                return q;
            } else {
                BlockingQueue<T> res = new LinkedBlockingQueue<>();
                res.add(message);
                return res;
            }
        });
    }

    @Override
    public T poll(String topic, String subscriberKey) {
        return poll(topic, subscriberKey, 0, TimeUnit.MILLISECONDS);
    }

    @Override
    public T poll(String topic, String subscriberKey, int timeout, TimeUnit unit) {
        BlockingQueue<T> messages = topics.get(topic);

        try {
            return messages.poll(timeout, unit);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterable<T> poll(String topic, String subscriberKey, int timeout, TimeUnit unit, int n) {
        BlockingQueue<T> messages = topics.get(topic);

        List<T> batch = new ArrayList<>();
        try {
            BlockingQueueBatcher.take(messages, batch, n, timeout, unit);
            return batch;
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
