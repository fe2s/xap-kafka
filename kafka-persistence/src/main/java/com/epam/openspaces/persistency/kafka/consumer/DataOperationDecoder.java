package com.epam.openspaces.persistency.kafka.consumer;

import kafka.serializer.Decoder;

import org.apache.commons.lang.SerializationUtils;

import com.epam.openspaces.persistency.kafka.protocol.DataOperation;

public class DataOperationDecoder implements Decoder<DataOperation> {

    @Override
    public DataOperation fromBytes(byte[] arg0) {
        return (DataOperation) SerializationUtils.deserialize(arg0);
    }
}
