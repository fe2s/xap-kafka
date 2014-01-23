package com.epam.openspaces.persistency.kafka.serializer;

import com.epam.openspaces.persistency.kafka.protocol.KafkaDataOperation;
import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;
import org.apache.commons.lang.SerializationUtils;

/**
 * Created by Oleksiy_Dyagilev
 */
public class KafkaDataOperationEncoder implements Encoder<KafkaDataOperation> {

    public KafkaDataOperationEncoder(VerifiableProperties props) {
        // TODO
    }

    @Override
    public byte[] toBytes(KafkaDataOperation dataOperation) {
        return SerializationUtils.serialize(dataOperation);
    }
}
