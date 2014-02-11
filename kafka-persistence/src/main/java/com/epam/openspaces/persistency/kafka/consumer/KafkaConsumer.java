package com.epam.openspaces.persistency.kafka.consumer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.epam.openspaces.persistency.kafka.protocol.impl.KafkaMessage;
import com.epam.openspaces.persistency.kafka.protocol.impl.KafkaMessageKey;
import com.epam.openspaces.persistency.kafka.protocol.impl.serializer.KafkaMessageDecoder;
import com.epam.openspaces.persistency.kafka.protocol.impl.serializer.KafkaMessageKeyDecoder;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;

/**
 * Wrapper around @{link ConsumerConnector} to help configure iterators with default XAP-Kafka message protocol.
 */
public class KafkaConsumer {

    private ConsumerConnector consumerConnector;

    public KafkaConsumer(ConsumerConnector consumerConnector) {
        this.consumerConnector = consumerConnector;
    }

    public ConsumerIterator<KafkaMessageKey, KafkaMessage> createIterator(String topic) {
        return createIterators(1, topic).get(0);
    }

    public List<ConsumerIterator<KafkaMessageKey, KafkaMessage>> createIterators(int count, String topic) {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, count);

        Map<String, List<KafkaStream<KafkaMessageKey, KafkaMessage>>> streams = consumerConnector.createMessageStreams(
                topicCountMap,
                new KafkaMessageKeyDecoder(),
                new KafkaMessageDecoder());

        List<KafkaStream<KafkaMessageKey, KafkaMessage>> kafkaStreams = streams.get(topic);

        List<ConsumerIterator<KafkaMessageKey, KafkaMessage>> iterators = new ArrayList<ConsumerIterator<KafkaMessageKey, KafkaMessage>>(count);

        for (KafkaStream<KafkaMessageKey, KafkaMessage> kafkaStream : kafkaStreams) {
            iterators.add(kafkaStream.iterator());
        }

        return iterators;
    }

    /**
     * Commit the offsets of all broker partitions connected by this connector.
     */
    public void commitOffsets(){
        consumerConnector.commitOffsets();
    }

    public ConsumerConnector getConsumerConnector() {
        return consumerConnector;
    }

}
