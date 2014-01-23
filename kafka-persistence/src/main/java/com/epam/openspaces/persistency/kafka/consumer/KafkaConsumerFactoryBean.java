package com.epam.openspaces.persistency.kafka.consumer;

import java.util.Properties;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.FactoryBean;

import com.epam.openspaces.persistency.kafka.KafkaSpaceSynchronizationEndpointFactoryBean;

public class KafkaConsumerFactoryBean implements FactoryBean<KafkaConsumer> {

    private static final Log logger = LogFactory
            .getLog(KafkaSpaceSynchronizationEndpointFactoryBean.class);

    private Properties consumerProperties;
    private ConsumerConnector consumer;

    @Override
    public KafkaConsumer getObject() throws Exception {
        logger.info("Initializing Kafka consumer");

        ConsumerConfig config = new ConsumerConfig(consumerProperties);
        consumer = Consumer.createJavaConsumerConnector(config);

        return new KafkaConsumer(consumer);
    }

    @Override
    public Class<?> getObjectType() {
        return KafkaConsumer.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    public void setConsumerProperties(Properties consumerProperties) {
        this.consumerProperties = consumerProperties;
    }

}
