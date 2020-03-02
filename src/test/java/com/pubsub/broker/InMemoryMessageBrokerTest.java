package com.pubsub.broker;

import com.pubsub.client.BatchMessageConsumer;
import com.pubsub.client.BrokerMessageProducer;
import com.pubsub.client.MessageConsumer;
import com.pubsub.client.User;
import java.util.Arrays;
import java.util.Iterator;
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

            Iterable<String> iterable = consumer.iterable();
            for (Object message : iterable) {
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

            Iterable<Iterable<String>> iterable = consumer.iterable();
            for (Iterable<String> messages : iterable) {
                for (String message : messages) {
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

    @Test
    public void testCustomObjectPubSubBatch() {
        BrokerMessageProducer producer = new BrokerMessageProducer(messageBroker);
        User user = new User();
        user.setName("name1");
        producer.send("user", user);

        User user2 = new User();
        user2.setName("name2");
        producer.send("user", user2);


        final BatchMessageConsumer consumer = new BatchMessageConsumer(
                messageBroker, "user", 1, TimeUnit.MILLISECONDS, 10);

        final Iterable<Iterable<User>> iterable = consumer.iterable();
        final Iterator<Iterable<User>> iterator = iterable.iterator();
        final Iterable<User> users = iterator.next();
        Iterator<User> userIterator = users.iterator();
        assertEquals(user, userIterator.next());
        assertEquals(user2, userIterator.next());
        final Iterable<User> users2 = iterator.next();
        assertNull(users2);
    }
}
