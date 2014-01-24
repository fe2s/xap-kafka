package com.epam.openspaces.persistency.kafka;

import com.epam.openspaces.persistency.kafka.serializer.KafkaMessageEncoder;
import kafka.producer.DefaultPartitioner;

import java.util.Properties;

/**
 * Created by Oleksiy_Dyagilev
 */
public class DefaultProducerProperties extends Properties {

    public DefaultProducerProperties() {
        put("serializer.class", KafkaMessageEncoder.class.getCanonicalName());
        put("partitioner.class", DefaultPartitioner.class.getCanonicalName());
    }

}
