package com.epam.openspaces.persistency.kafka;

import java.util.Properties;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;

import com.epam.openspaces.persistency.kafka.consumer.KafkaConsumer;
import com.epam.openspaces.persistency.kafka.protocol.KafkaMessage;

public class TestConsumerTask extends Thread {
    private String topic;
    private KafkaConsumer consumer;
    private int objectCount;

    public TestConsumerTask(String topic, int objectCount) {
        this.objectCount = objectCount;
        consumer = new KafkaConsumer(
                Consumer.createJavaConsumerConnector(createConsumerConfig()));

        this.topic = topic;
    }

    private static ConsumerConfig createConsumerConfig() {
        Properties props = new Properties();
        props.put("zookeeper.connect", "localhost:2181");
        props.put("group.id", "0");
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");

        return new ConsumerConfig(props);

    }

    @Override
    public void run() {
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        ConsumerIterator<String, KafkaMessage> iterator = consumer
                .createIterator(topic);
        int i = 1;
        while (iterator.hasNext()) {
            System.out.println(iterator.next().message());
            if (i >= objectCount) {
                break;
            }
            i++;
        }
    }
}
