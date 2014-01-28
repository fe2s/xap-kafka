package com.epam.openspaces.persistency.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;

public class Consumer extends Thread {
    private final ConsumerConnector consumer;
    private final String topic;

    public Consumer(String topic) {
        consumer = kafka.consumer.Consumer
                .createJavaConsumerConnector(createConsumerConfig());
        this.topic = topic;
    }

    private static ConsumerConfig createConsumerConfig() {
        Properties props = new Properties();
        props.put("zookeeper.connect", KafkaProperties.zkConnect);
        props.put("group.id", KafkaProperties.groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");

        return new ConsumerConfig(props);

    }

    @Override
    public void run() {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(1));
        Map<String, List<KafkaStream<String, String>>> streams = consumer
                .createMessageStreams(topicCountMap, new StringDecoder(null),
                        new StringDecoder(null));

        List<KafkaStream<String, String>> kafkaStreams = streams.get(topic);

        ConsumerIterator<String, String> iterator = kafkaStreams.get(0)
                .iterator();

        while (iterator.hasNext()) {
            System.out.println(iterator.next().message());
        }
    }
}
