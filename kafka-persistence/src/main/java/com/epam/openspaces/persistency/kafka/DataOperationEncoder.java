package com.epam.openspaces.persistency.kafka;

import com.epam.openspaces.persistency.kafka.protocol.DataOperation;
import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;
import org.apache.commons.lang.SerializationUtils;

import java.io.Serializable;

/**
 * Created by Oleksiy_Dyagilev
 */
public class DataOperationEncoder implements Encoder<DataOperation> {

    public DataOperationEncoder(VerifiableProperties props) {
        // TODO
    }

    @Override
    public byte[] toBytes(DataOperation dataOperation) {
        return SerializationUtils.serialize(dataOperation);
    }
}
