package com.epam.openspaces.persistency.kafka;

import com.epam.openspaces.persistency.kafka.serializer.KafkaDataOperationEncoder;
import kafka.producer.DefaultPartitioner;

import java.util.Properties;

/**
 * Created by Oleksiy_Dyagilev
 */
public class DefaultProducerProperties extends Properties {

    public DefaultProducerProperties() {
        put("serializer.class", KafkaDataOperationEncoder.class.getCanonicalName());
        put("partitioner.class", DefaultPartitioner.class.getCanonicalName());
    }

}
