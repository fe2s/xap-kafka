package com.epam.openspaces.persistency.kafka;

import com.epam.openspaces.persistency.kafka.protocol.impl.KafkaMessage;
import com.epam.openspaces.persistency.kafka.protocol.impl.KafkaMessageKey;
import com.epam.openspaces.persistency.kafka.protocol.impl.serializer.KafkaMessageEncoder;
import com.epam.openspaces.persistency.kafka.protocol.impl.serializer.KafkaMessageKeyEncoder;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import kafka.producer.DefaultPartitioner;

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
    private Producer<KafkaMessageKey, KafkaMessage> producer;

    private String spaceDocumentKafkaTopicPropertyName = "spaceDocument.kafka.topic";

    @Override
    public KafkaSpaceSynchronizationEndpoint getObject() throws Exception {
        logger.info("Initializing Kafka producer");

        Properties combinedProducerProps = applyDefaultProducerProperties();

        Config synchronizationEndpointConfig = createSynchronizationEndpointConfig();
        ProducerConfig producerConfig = new ProducerConfig(combinedProducerProps);

        this.producer = new Producer<KafkaMessageKey, KafkaMessage>(producerConfig);

        return new KafkaSpaceSynchronizationEndpoint(this.producer, synchronizationEndpointConfig);
    }

    private Config createSynchronizationEndpointConfig() {
        Config config = new Config();
        config.setSpaceDocumentKafkaTopicPropertyName(spaceDocumentKafkaTopicPropertyName);
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

    public void setSpaceDocumentKafkaTopicPropertyName(String spaceDocumentKafkaTopicPropertyName) {
        this.spaceDocumentKafkaTopicPropertyName = spaceDocumentKafkaTopicPropertyName;
    }

    @Override
    public void destroy() throws Exception {
        logger.info("Closing Kafka producer");
        if (this.producer != null) {
            producer.close();
        }
    }

    /**
     * Default producer properties to configure XAP-Kafka protocol
     */
    class DefaultProducerProperties extends Properties {

        public DefaultProducerProperties() {
            put("key.serializer.class", KafkaMessageKeyEncoder.class.getCanonicalName());
            put("serializer.class", KafkaMessageEncoder.class.getCanonicalName());
            put("partitioner.class", DefaultPartitioner.class.getCanonicalName());
        }

    }
}
