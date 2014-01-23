package com.epam.openspaces.persistency.kafka.consumer;

import kafka.consumer.ConsumerIterator;

import com.epam.openspaces.persistency.kafka.protocol.KafkaDataOperation;

public class KafkaDataOperationIterator {

    private ConsumerIterator<String, KafkaDataOperation> consumerIterator;

    public KafkaDataOperationIterator(ConsumerIterator<String, KafkaDataOperation> iterator) {
        consumerIterator = iterator;
    }

    public boolean hasNext() {
        return consumerIterator.hasNext();
    }

}
