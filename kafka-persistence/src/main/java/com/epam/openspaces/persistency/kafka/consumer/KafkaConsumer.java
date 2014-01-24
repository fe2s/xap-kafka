package com.epam.openspaces.persistency.kafka.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.epam.openspaces.persistency.kafka.serializer.KafkaMessageDecoder;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;

import com.epam.openspaces.persistency.kafka.protocol.KafkaMessage;

public class KafkaConsumer {

    private ConsumerConnector kafkaConsumer;

    public KafkaConsumer(ConsumerConnector consumer) {
        kafkaConsumer = consumer;
    }

    public ConsumerIterator<String, KafkaMessage> getKafkaIterator(String topic) {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(1));
        Map<String, List<KafkaStream<String, KafkaMessage>>> streams = kafkaConsumer
                .createMessageStreams(topicCountMap, new StringDecoder(null),
                        new KafkaMessageDecoder());

        List<KafkaStream<String, KafkaMessage>> kafkaStreams = streams
                .get(topic);

        ConsumerIterator<String, KafkaMessage> iterator = kafkaStreams
                .get(0).iterator();

        return iterator;
    }
}
