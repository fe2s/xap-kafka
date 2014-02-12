package com.epam.openspaces.persistency.kafka;

import com.epam.openspaces.persistency.kafka.annotations.KafkaTopic;
import com.epam.openspaces.persistency.kafka.protocol.impl.KafkaMessage;
import com.epam.openspaces.persistency.kafka.protocol.impl.KafkaMessageFactory;
import com.epam.openspaces.persistency.kafka.protocol.impl.KafkaMessageKey;
import kafka.javaapi.producer.Producer;
import org.springframework.core.annotation.AnnotationUtils;

/**
 * Default implementation of Space Synchronization Endpoint which uses Apache Kafka as external data store.
 * Space synchronization operations are converted to XAP-Kafka protocol and sent to Kafka server.
 *
 * @author Oleksiy_Dyagilev
 */
public class KafkaSpaceSynchronizationEndpoint extends AbstractKafkaSpaceSynchronizationEndpoint<KafkaMessageKey, KafkaMessage> {

    private Config config;

    public KafkaSpaceSynchronizationEndpoint(Producer<KafkaMessageKey, KafkaMessage> kafkaProducer, Config config) {
        this.kafkaMessageFactory = new KafkaMessageFactory();
        this.kafkaProducer = kafkaProducer;
        this.config = config;
    }

    /**
     * inspects original data class for @KafkaTopic annotation
     */
    protected String resolveTopicForMessage(KafkaMessage message) {
        if (message.hasDataAsObject()) {
            // consider perf optimization with a cache
            Object data = message.getDataAsObject();
            KafkaTopic kafkaTopic = AnnotationUtils.findAnnotation(data.getClass(), KafkaTopic.class);
            if (kafkaTopic == null) {
                return null;
            } else {
                return kafkaTopic.value();
            }
        } else {
            Object topic = message.getDataAsMap().get(config.getSpaceDocumentKafkaTopicName());
            return topic != null ? topic.toString() : null;
        }
    }
    public static class Config {
        private String spaceDocumentKafkaTopicName;

        public String getSpaceDocumentKafkaTopicName() {
            return spaceDocumentKafkaTopicName;
        }

        public void setSpaceDocumentKafkaTopicName(String spaceDocumentKafkaTopicName) {
            this.spaceDocumentKafkaTopicName = spaceDocumentKafkaTopicName;
        }
    }

}
