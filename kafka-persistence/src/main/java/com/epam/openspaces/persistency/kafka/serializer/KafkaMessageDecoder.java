package com.epam.openspaces.persistency.kafka.serializer;

import kafka.serializer.Decoder;

import org.apache.commons.lang.SerializationUtils;

import com.epam.openspaces.persistency.kafka.protocol.KafkaMessage;

/**
 * Decoder for XAP-Kafka protocol.
 *
 * @see com.epam.openspaces.persistency.kafka.protocol.KafkaMessage
 */
public class KafkaMessageDecoder implements Decoder<KafkaMessage> {

    @Override
    public KafkaMessage fromBytes(byte[] bytes) {
        return (KafkaMessage) SerializationUtils.deserialize(bytes);
    }
}
