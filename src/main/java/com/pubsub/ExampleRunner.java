package com.pubsub;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pubsub.broker.InMemoryMessageBroker;
import com.pubsub.broker.MessageBroker;
import com.pubsub.client.BrokerMessageProducer;
import com.pubsub.client.MessageConsumer;
import com.pubsub.client.User;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ExampleRunner {

    private static final int PRODUCER_NUMBER = 5;

    private static final int CONSUMER_NUMBER = 25;

    private static final Random random = new Random();

    public static void main(String[] args) throws InterruptedException {
//        List<String> topics = Arrays.asList("topic1");
        List<String> topics = Arrays.asList("topic1", "topic2", "topic3");

        final MessageBroker messageBroker = new InMemoryMessageBroker();
        topics.forEach(messageBroker::createTopic);

        final ObjectMapper objectMapper = new ObjectMapper();
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(10);

        for (int i = 0; i < PRODUCER_NUMBER; i++) {
            final BrokerMessageProducer producer = new BrokerMessageProducer(
                    messageBroker, objectMapper
            );
            int index = random.nextInt(topics.size());
            final String topic = topics.get(index);

            scheduler.scheduleAtFixedRate(() -> {
                producer.send(topic, randomUser());
            }, 10, 100, TimeUnit.MILLISECONDS);
        }

        while (true) {
            System.out.println("size : " + messageBroker.size());
            for (int i = 0; i < CONSUMER_NUMBER; i++) {
                int index = random.nextInt(topics.size());
                final String topic = topics.get(index);
                final MessageConsumer consumer = new MessageConsumer(messageBroker, topic);
                for (int j = 0; j < 15; j++) {
                    scheduler.schedule(() -> consumer.consume(10, TimeUnit.MILLISECONDS), 2 * j, TimeUnit.MILLISECONDS);
                }

            }
            Thread.sleep(4_000);
        }

    }

    private static User randomUser() {
        User user = new User();
        user.setAge(12);
        user.setName("name");
        return user;
    }

}
