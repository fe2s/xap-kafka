package com.epam.openspaces.persistency.kafka;

import com.epam.openspaces.persistency.kafka.annotations.KafkaTopic;
import com.epam.openspaces.persistency.kafka.protocol.KafkaDataOperation;
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

/**
 * Created by Oleksiy_Dyagilev
 */
public class KafkaSpaceSynchronizationEndpoint extends SpaceSynchronizationEndpoint {

    private static final Log logger = LogFactory.getLog(KafkaSpaceSynchronizationEndpoint.class);

    private final Producer<String, KafkaDataOperation> kafkaProducer;

    public KafkaSpaceSynchronizationEndpoint(Producer<String, KafkaDataOperation> kafkaProducer) {
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
        // TODO: batch/buffering
        for (DataSyncOperation dataSyncOperation : transactionParticipantDataItems) {
            try {
                KafkaDataOperation dataOperation = KafkaDataOperationFactory.create(dataSyncOperation);
                String uid = dataSyncOperation.getUid();

                writeToKafka(dataOperation);
            } catch (KafkaPersistenceException e) {
                logger.error("Exception during Kafka protocol object creation. This data operation will not be persisted", e);
            }
        }
    }

    // TODO: need to write key for partitioning
    protected void writeToKafka(KafkaDataOperation message) {

        String topic = resolveTopicForMessage(message);
        if (StringUtils.isEmpty(topic)) {
            if (logger.isTraceEnabled()) {
                logger.trace("Topic for message not found. Message will be filtered out. " + message);
            }
            return;
        }

        if (logger.isTraceEnabled()) {
            logger.trace("Writing to Kafka. message = " + message);
        }

        kafkaProducer.send(new KeyedMessage<String, KafkaDataOperation>(topic, message));
    }

    protected String resolveTopicForMessage(KafkaDataOperation message) {
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
