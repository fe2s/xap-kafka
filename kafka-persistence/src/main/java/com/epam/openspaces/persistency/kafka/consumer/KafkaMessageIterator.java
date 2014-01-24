package com.epam.openspaces.persistency.kafka.consumer;

import com.epam.openspaces.persistency.kafka.protocol.KafkaMessage;
import kafka.consumer.ConsumerIterator;

public class KafkaMessageIterator {

    private ConsumerIterator<String, KafkaMessage> consumerIterator;

    public KafkaMessageIterator(ConsumerIterator<String, KafkaMessage> iterator) {
        consumerIterator = iterator;
    }

    public boolean hasNext() {
        return consumerIterator.hasNext();
    }

}
