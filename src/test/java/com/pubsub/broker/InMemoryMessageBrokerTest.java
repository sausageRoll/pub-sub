package com.pubsub.broker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pubsub.model.Message;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class InMemoryMessageBrokerTest {

    List<String> topics = Arrays.asList("topic1", "topic2", "topic3");

    final MessageBroker messageBroker = new InMemoryMessageBroker();

    final ObjectMapper objectMapper = new ObjectMapper();

    final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(10);

    @BeforeEach
    public void setUp() {
        topics.forEach(messageBroker::createTopic);
    }

    @Test
    public void testReadWriteSingle() {
        new Thread(() -> {
            for (int i = 0; i < 1000; i++) {
                messageBroker.publishMessage("topic1", new Message<>("message"));
            }
        }).start();

        new Thread(() -> {
            for (int i = 0; i < 1000; i++) {
                String key = messageBroker.subscribe("topic1");
                Message<String> message = messageBroker.poll("topic1", key, 10, TimeUnit.MILLISECONDS);
                System.out.println(message.getValue());
            }
        }).start();
    }

    @Test
    public void testReadWriteBatch() {
        new Thread(() -> {
            for (int i = 0; i < 1000; i++) {
                messageBroker.publishMessage("topic1", new Message<>("message"));
            }
        }).start();

        new Thread(() -> {
            for (int i = 0; i < 10; i++) {
                String key = messageBroker.subscribe("topic1");
                Iterable<Message<String>> messages = messageBroker.poll("topic1", key, 10, TimeUnit.MILLISECONDS, 100);
                messages.forEach((message) -> System.out.println(message.getValue()));
            }
        }).start();
    }

    @Test
    public void testPublishNonExistTopic() {
        messageBroker.publishMessage("not-existing", new Message<>("message"));

        String key = messageBroker.subscribe("not-existing");
        assertEquals(messageBroker.poll("not-existing", key).getValue(), "message");
    }

    @Test
    public void testTimeOut() {
        messageBroker.publishMessage("topic1", new Message<>("message"));

        String key = messageBroker.subscribe("topic1");
        assertEquals(messageBroker.poll("topic1", key).getValue(), "message");
        assertNull(messageBroker.poll("topic1", key, 1, TimeUnit.SECONDS));
    }

    @Test
    public void testCreateSubscriberOnHandeledTopic() {
        messageBroker.publishMessage("topic1", new Message<>("message"));

        String key = messageBroker.subscribe("topic1");
        assertThrows(IllegalStateException.class, () -> messageBroker.subscribe("topic1"));
    }
}
