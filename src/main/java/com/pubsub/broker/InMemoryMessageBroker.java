package com.pubsub.broker;

import com.pubsub.model.Message;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

public class InMemoryMessageBroker implements MessageBroker {

    private final Map<String, Integer> committedOffset = new ConcurrentHashMap<>();

    private final Map<String, CopyOnWriteArrayList<Message<String>>> topics = new ConcurrentHashMap<>();

    @Override
    public boolean createTopic(String topicName) {
        return topics.putIfAbsent(topicName, new CopyOnWriteArrayList<>()) == null;
    }

    @Override
    public String subscribe(String topic) {
        String subscriberKey = UUID.randomUUID().toString();
        committedOffset.put(topic + subscriberKey, 0);
        return subscriberKey;
    }

    @Override
    public boolean unsubscribe(String topic, String subscriberKey) {
        return committedOffset.remove(topic + subscriberKey) != null;
    }

    @Override
    public void publishMessage(String topic, Message<String> message) {
        System.out.println(String.format("message %s came to topic %s", message.getValue(), topic));
        topics.compute(topic, (t, q) -> {
            if (q != null) {
                q.add(message);
                return q;
            } else {
                CopyOnWriteArrayList<Message<String>> res = new CopyOnWriteArrayList<>();
                res.add(message);
                return res;
            }
        });
    }

    @Override
    public Message<String> poll(String topic, String subscriberKey) {
        return poll(topic, subscriberKey, 0, TimeUnit.MILLISECONDS);
    }

    @Override
    public Message<String> poll(String topic, String subscriberKey, int timeout, TimeUnit unit) {
        Integer offset = committedOffset.get(topic + subscriberKey);
        if (offset == null) {
            return null;
        }
        if (topics.get(topic).size() > offset) {
            return topics.get(topic).get(offset);
        } else {
            return null;
        }
    }

    @Override
    public boolean commitOffset(String topic, String subscriberKey) {
        return committedOffset.computeIfPresent(topic + subscriberKey, (t, offset) -> offset + 1) != null;
    }

    @Override
    public int size() {
        return topics.values().stream()
                .mapToInt(Collection::size)
                .sum();
    }
}
