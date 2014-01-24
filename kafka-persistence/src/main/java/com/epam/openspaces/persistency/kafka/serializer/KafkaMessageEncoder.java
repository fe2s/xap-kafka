package com.epam.openspaces.persistency.kafka.serializer;

import com.epam.openspaces.persistency.kafka.protocol.KafkaMessage;
import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;
import org.apache.commons.lang.SerializationUtils;

/**
 * Created by Oleksiy_Dyagilev
 */
public class KafkaMessageEncoder implements Encoder<KafkaMessage> {

    public KafkaMessageEncoder(VerifiableProperties props) {
        // TODO
    }

    @Override
    public byte[] toBytes(KafkaMessage dataOperation) {
        return SerializationUtils.serialize(dataOperation);
    }
}
