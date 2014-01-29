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

/**
 * Wrapper around @{link ConsumerConnector} to help configure iterators with XAP-Kafka message decoder.
 */
public class KafkaConsumer {

    private ConsumerConnector consumerConnector;

    public KafkaConsumer(ConsumerConnector consumerConnector) {
        this.consumerConnector = consumerConnector;
    }

    public ConsumerIterator<String, KafkaMessage> createIterator(String topic) {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, 1);

        Map<String, List<KafkaStream<String, KafkaMessage>>> streams = consumerConnector.createMessageStreams(
                topicCountMap,
                new StringDecoder(null),
                new KafkaMessageDecoder());

        List<KafkaStream<String, KafkaMessage>> kafkaStreams = streams.get(topic);

        ConsumerIterator<String, KafkaMessage> iterator = kafkaStreams.get(0).iterator();

        return iterator;
    }

    public ConsumerConnector getConsumerConnector() {
        return consumerConnector;
    }

}
