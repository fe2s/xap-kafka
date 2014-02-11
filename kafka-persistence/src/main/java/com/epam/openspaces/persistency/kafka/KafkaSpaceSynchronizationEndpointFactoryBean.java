package com.epam.openspaces.persistency.kafka;

import com.epam.openspaces.persistency.kafka.protocol.KafkaMessage;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;

import java.util.Properties;

import static com.epam.openspaces.persistency.kafka.KafkaSpaceSynchronizationEndpoint.Config;

/**
 * A factory bean which creates {@link KafkaSpaceSynchronizationEndpoint} with given producer properties.
 *
 * @author Oleksiy_Dyagilev
 */
public class KafkaSpaceSynchronizationEndpointFactoryBean implements FactoryBean<KafkaSpaceSynchronizationEndpoint>, DisposableBean {

    private static final Log logger = LogFactory.getLog(KafkaSpaceSynchronizationEndpointFactoryBean.class);

    private Properties producerProperties;
    private Producer<String, KafkaMessage> producer;

    private String spaceDocumentKafkaTopicName = "aaa";

    @Override
    public KafkaSpaceSynchronizationEndpoint getObject() throws Exception {
        logger.info("Initializing Kafka producer");

        Properties combinedProducerProps = applyDefaultProducerProperties();

        Config synchronizationEndpointConfig = aplySynchronizationEndpointConfig();
        ProducerConfig config = new ProducerConfig(combinedProducerProps);
        this.producer = new Producer<String, KafkaMessage>(config);

        return new KafkaSpaceSynchronizationEndpoint(this.producer, synchronizationEndpointConfig);
    }

    private Config aplySynchronizationEndpointConfig() {
        Config config = new Config();
        config.setSpaceDocumentKafkaTopicName(spaceDocumentKafkaTopicName);
        return config;
    }

    protected Properties applyDefaultProducerProperties() {
        DefaultProducerProperties combined = new DefaultProducerProperties();
        combined.putAll(producerProperties);
        return combined;
    }

    @Override
    public Class<?> getObjectType() {
        return KafkaSpaceSynchronizationEndpoint.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    public void setProducerProperties(Properties producerProperties) {
        this.producerProperties = producerProperties;
    }

    public void setSpaceDocumentKafkaTopicName(String spaceDocumentKafkaTopicName) {
        this.spaceDocumentKafkaTopicName = spaceDocumentKafkaTopicName;
    }

    @Override
    public void destroy() throws Exception {
        logger.info("Closing Kafka producer");
        if (this.producer != null) {
            producer.close();
        }
    }
}
