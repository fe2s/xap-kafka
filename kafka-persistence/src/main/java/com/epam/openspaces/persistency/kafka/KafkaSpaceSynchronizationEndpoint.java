package com.epam.openspaces.persistency.kafka;

import com.epam.openspaces.persistency.kafka.annotations.KafkaTopic;
import com.epam.openspaces.persistency.kafka.protocol.KafkaMessage;
import com.gigaspaces.sync.DataSyncOperation;
import com.gigaspaces.sync.OperationsBatchData;
import com.gigaspaces.sync.SpaceSynchronizationEndpoint;
import com.gigaspaces.sync.TransactionData;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.core.annotation.AnnotationUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * An implementation of Space Synchronization Endpoint which uses Apache Kafka as external data store.
 * Space synchronization operations are converted to Kafka protocol and sent to server.
 *
 * @author Oleksiy_Dyagilev
 */
public class KafkaSpaceSynchronizationEndpoint extends SpaceSynchronizationEndpoint {

    private static final Log logger = LogFactory.getLog(KafkaSpaceSynchronizationEndpoint.class);

    private final Producer<String, KafkaMessage> kafkaProducer;

    public KafkaSpaceSynchronizationEndpoint(Producer<String, KafkaMessage> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    @Override
    public void onTransactionSynchronization(TransactionData transactionData) {
        executeDataSyncOperations(transactionData.getTransactionParticipantDataItems());
    }

    @Override
    public void onOperationsBatchSynchronization(OperationsBatchData batchData) {
        executeDataSyncOperations(batchData.getBatchDataItems());
    }

    protected void executeDataSyncOperations(DataSyncOperation[] transactionParticipantDataItems) {
        List<KafkaMessage> kafkaMessages = convertToKafkaMessages(transactionParticipantDataItems);
        sendToKafka(kafkaMessages);
    }

    /**
     * converts XAP data sync operations to Kafka messages (protocol objects)
     */
    private List<KafkaMessage> convertToKafkaMessages(DataSyncOperation[] transactionParticipantDataItems) {
        List<KafkaMessage> kafkaMessages = new ArrayList<KafkaMessage>(transactionParticipantDataItems.length);
        for (DataSyncOperation dataSyncOperation : transactionParticipantDataItems) {
            try {
                KafkaMessage message = KafkaMessageFactory.create(dataSyncOperation);
                kafkaMessages.add(message);
            } catch (KafkaPersistenceException e) {
                logger.error("Exception during Kafka protocol object creation. This data operation will not be persisted", e);
            }
        }
        return kafkaMessages;
    }

    // TODO: need to write key for partitioning

    /**
     * sends given messages to Kafka server
     */
    protected void sendToKafka(List<KafkaMessage> kafkaMessages) {
        List<KeyedMessage<String, KafkaMessage>> keyedMessages = new ArrayList<KeyedMessage<String, KafkaMessage>>(kafkaMessages.size());

        for (KafkaMessage message : kafkaMessages) {
            String topic = resolveTopicForMessage(message);
            if (StringUtils.isEmpty(topic)) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Topic for message not found. Message will be filtered out. " + message);
                }
            } else {
                if (logger.isTraceEnabled()) {
                    logger.trace("Writing to Kafka " + message);
                }

                keyedMessages.add(new KeyedMessage<String, KafkaMessage>(topic, message));
            }
        }

        kafkaProducer.send(keyedMessages);
    }

    /**
     * inspects original data class for KafkaTopic annotation
     */
    protected String resolveTopicForMessage(KafkaMessage message) {
        if (message.hasDataAsObject()) {
            // TODO: optimize with a cache
            Object data = message.getDataAsObject();
            KafkaTopic kafkaTopic = AnnotationUtils.findAnnotation(data.getClass(), KafkaTopic.class);
            if (kafkaTopic == null) {
                return null;
            } else {
                return kafkaTopic.value();
            }
        } else {
            // TODO: topic for space document
            return null;
        }
    }

}
