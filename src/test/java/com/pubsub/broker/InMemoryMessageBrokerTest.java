package com.pubsub.broker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pubsub.client.BrokerMessageProducer;
import com.pubsub.client.MessageConsumer;
import com.pubsub.client.User;
import com.pubsub.model.Message;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.omg.CORBA.TIMEOUT;

import static org.junit.jupiter.api.Assertions.*;

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
    public void doubleReadCheck() {
        final BrokerMessageProducer producer = new BrokerMessageProducer(
                messageBroker, objectMapper
        );
        final MessageConsumer consumer = new MessageConsumer(messageBroker, "topic1");

        assertNull(consumer.consume(0, TimeUnit.MILLISECONDS));

        User user = new User();
        user.setName("name1");
        producer.send("topic1", user);
        User user2 = new User();
        user2.setName("name2");
        producer.send("topic1", user2);

        assertEquals("{\"name\":\"name1\",\"age\":0}",
                consumer.consume(0, TimeUnit.MILLISECONDS).getValue());
        consumer.stop();
        consumer.restart();
        assertEquals("{\"name\":\"name1\",\"age\":0}",
                consumer.consume(0, TimeUnit.MILLISECONDS).getValue());
        assertEquals("{\"name\":\"name2\",\"age\":0}",
                consumer.consume(0, TimeUnit.MILLISECONDS).getValue());
    }

    @Test
    public void testPollTimeout() {
        final BrokerMessageProducer producer = new BrokerMessageProducer(
                messageBroker, objectMapper
        );
        final MessageConsumer consumer = new MessageConsumer(messageBroker, "topic1");
        User user = new User();
        user.setName("name1");

        producer.send("topic1", user);
        consumer.consume(0, TimeUnit.MILLISECONDS);
        consumer.consume(0, TimeUnit.MILLISECONDS);

        scheduler.schedule(() -> producer.send("topic1", user), 10, TimeUnit.MILLISECONDS);
        assertNull(consumer.consume(0, TimeUnit.MILLISECONDS));
        assertNull(consumer.consume(0, TimeUnit.MILLISECONDS));
        assertNull(consumer.consume(0, TimeUnit.MILLISECONDS));
        assertNotNull(consumer.consume(100, TimeUnit.MILLISECONDS));
    }

}
