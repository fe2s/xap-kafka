package com.epam.openspaces.persistency.kafka;

import com.epam.openspaces.persistency.kafka.protocol.DataOperation;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;

import java.util.Properties;

/**
 * A factory bean which creates {@link KafkaSpaceSynchronizationEndpoint}
 *
 * Created by Oleksiy_Dyagilev
 */
public class KafkaSpaceSynchronizationEndpointFactoryBean implements FactoryBean<KafkaSpaceSynchronizationEndpoint>, DisposableBean {

    private static final Log logger = LogFactory.getLog(KafkaSpaceSynchronizationEndpointFactoryBean.class);

    private Properties producerProperties;
    private Producer<String, DataOperation> producer;

    @Override
    public KafkaSpaceSynchronizationEndpoint getObject() throws Exception {
        logger.info("Initializing Kafka producer");

        // TODO: we need default properties like default encoder
        ProducerConfig config = new ProducerConfig(producerProperties);
        this.producer = new Producer<String, DataOperation>(config);

        return new KafkaSpaceSynchronizationEndpoint(this.producer);
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

    @Override
    public void destroy() throws Exception {
        logger.info("Closing Kafka producer");
        if (this.producer != null) {
            producer.close();
        }
    }
}
