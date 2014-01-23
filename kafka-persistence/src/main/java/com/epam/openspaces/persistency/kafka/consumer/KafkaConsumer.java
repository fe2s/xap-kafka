package com.epam.openspaces.persistency.kafka.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;

import com.epam.openspaces.persistency.kafka.protocol.DataOperation;

public class KafkaConsumer {

    private ConsumerConnector kafkaConsumer;

    public KafkaConsumer(ConsumerConnector consumer) {
        kafkaConsumer = consumer;
    }

    public ConsumerIterator<String, DataOperation> getKafkaIterator(String topic) {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(1));
        Map<String, List<KafkaStream<String, DataOperation>>> streams = kafkaConsumer
                .createMessageStreams(topicCountMap, new StringDecoder(null),
                        new DataOperationDecoder());

        List<KafkaStream<String, DataOperation>> kafkaStreams = streams
                .get(topic);

        ConsumerIterator<String, DataOperation> iterator = kafkaStreams.get(0)
                .iterator();

        return iterator;
    }
}
