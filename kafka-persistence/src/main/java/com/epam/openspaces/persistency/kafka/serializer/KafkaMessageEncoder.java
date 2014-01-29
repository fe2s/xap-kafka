package com.epam.openspaces.persistency.kafka.serializer;

import com.epam.openspaces.persistency.kafka.protocol.KafkaMessage;
import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;
import org.apache.commons.lang.SerializationUtils;

/**
 * Encoder for XAP-Kafka protocol.
 *
 * @see com.epam.openspaces.persistency.kafka.protocol.KafkaMessage
 *
 * @author Oleksiy_Dyagilev
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
