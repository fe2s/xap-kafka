package com.epam.openspaces.persistency.kafka.consumer;

import kafka.consumer.ConsumerIterator;

import com.epam.openspaces.persistency.kafka.protocol.DataOperation;

public class KafkaDataOperationIterator {

    private ConsumerIterator<String, DataOperation> consumerIterator;

    public KafkaDataOperationIterator(
            ConsumerIterator<String, DataOperation> iterator) {
        consumerIterator = iterator;
    }

    public boolean hasNext() {
        return consumerIterator.hasNext();
    }

}
