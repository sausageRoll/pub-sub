package com.pubsub.broker;

import com.pubsub.client.BatchMessageConsumer;
import com.pubsub.client.MessageConsumer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class InMemoryMessageBrokerTest {

    List<String> topics = Arrays.asList("topic1", "topic2", "topic3");

    final MessageBroker messageBroker = new InMemoryMessageBroker();

    @BeforeEach
    public void setUp() {
        topics.forEach(messageBroker::createTopic);
    }

    @Test
    public void testReadWriteSingle() throws InterruptedException {
        Thread producer = new Thread(() -> {
            for (int i = 0; i < 1000; i++) {
                messageBroker.publishMessage("topic1", "message");
            }
        });
        producer.start();

        Thread consumerThread = new Thread(() -> {
            final MessageConsumer consumer = new MessageConsumer(
                    messageBroker, "topic1", 10, TimeUnit.MILLISECONDS);

            for (Object message : consumer) {
                if (message != null) {
                    System.out.println(String.format(
                            "consumed message %s from topic %s",
                            message, "topic1"));
                } else {
                    System.out.println(String.format("topic %s is empty", "topic1"));
                }
            }
        });
        consumerThread.start();

        Thread.sleep(10_000);

        consumerThread.interrupt();
        producer.interrupt();
    }

    @Test
    public void testReadWriteBatch() throws InterruptedException {
        Thread producer = new Thread(() -> {
            for (int i = 0; i < 1000; i++) {
                messageBroker.publishMessage("topic1", "message");
            }
        });
        producer.start();

        Thread consumerThread = new Thread(() -> {
            final BatchMessageConsumer consumer = new BatchMessageConsumer(
                    messageBroker, "topic1", 10, TimeUnit.MILLISECONDS, 10);

            for (Iterable messages : consumer) {
                for (Object message : messages) {
                    if (message != null) {
                        System.out.println(String.format(
                                "consumed message %s from topic %s",
                                message, "topic1"));
                    } else {
                        System.out.println(String.format("topic %s is empty", "topic1"));
                    }
                }
            }
        });
        consumerThread.start();

        Thread.sleep(10_000);

        consumerThread.interrupt();
        producer.interrupt();
    }

    @Test
    public void testPublishNonExistTopic() {
        messageBroker.publishMessage("not-existing", "message");

        String key = messageBroker.subscribe("not-existing");
        assertEquals(messageBroker.poll("not-existing", key), "message");
    }

    @Test
    public void testTimeOut() {
        messageBroker.publishMessage("topic1", "message");

        String key = messageBroker.subscribe("topic1");
        assertEquals(messageBroker.poll("topic1", key), "message");
        assertNull(messageBroker.poll("topic1", key, 1, TimeUnit.SECONDS));
    }

    @Test
    public void testCreateSubscriberOnHandeledTopic() {
        messageBroker.publishMessage("topic1", "message");

        String key = messageBroker.subscribe("topic1");
        assertThrows(IllegalStateException.class, () -> messageBroker.subscribe("topic1"));
    }
}
