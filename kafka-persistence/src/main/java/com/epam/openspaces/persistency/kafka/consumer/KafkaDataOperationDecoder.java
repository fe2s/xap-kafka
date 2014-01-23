package com.epam.openspaces.persistency.kafka.consumer;

import kafka.serializer.Decoder;

import org.apache.commons.lang.SerializationUtils;

import com.epam.openspaces.persistency.kafka.protocol.KafkaDataOperation;

public class KafkaDataOperationDecoder implements Decoder<KafkaDataOperation> {

    @Override
    public KafkaDataOperation fromBytes(byte[] arg0) {
        return (KafkaDataOperation) SerializationUtils.deserialize(arg0);
    }
}
