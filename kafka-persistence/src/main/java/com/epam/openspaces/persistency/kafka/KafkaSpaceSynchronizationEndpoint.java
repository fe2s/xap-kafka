package com.epam.openspaces.persistency.kafka;

import com.epam.openspaces.persistency.kafka.protocol.DataOperation;
import com.gigaspaces.sync.DataSyncOperation;
import com.gigaspaces.sync.OperationsBatchData;
import com.gigaspaces.sync.SpaceSynchronizationEndpoint;
import com.gigaspaces.sync.TransactionData;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import scala.util.parsing.combinator.testing.Str;

/**
 * Created by Oleksiy_Dyagilev
 */
public class KafkaSpaceSynchronizationEndpoint extends SpaceSynchronizationEndpoint {

    private static final Log logger = LogFactory.getLog(KafkaSpaceSynchronizationEndpoint.class);

    private final Producer<String, DataOperation> kafkaProducer;

    public KafkaSpaceSynchronizationEndpoint(Producer<String, DataOperation> kafkaProducer) {
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

    private void executeDataSyncOperations(DataSyncOperation[] transactionParticipantDataItems) {
        // TODO: batch/buffering
        for (DataSyncOperation dataSyncOperation : transactionParticipantDataItems) {
            try {
                DataOperation dataOperation = DataOperationFactory.create(dataSyncOperation);
                String uid = dataSyncOperation.getUid();

                writeToKafka(dataOperation);
            } catch (KafkaPersistenceException e) {
                logger.error("Exception during Kafka protocol object creation. This data operation will not be persisted", e);
            }
        }
    }

    // TODO: need to write key for partiotioning
    private void writeToKafka(DataOperation message) {
        if (logger.isTraceEnabled()) {
            logger.trace("Writing to Kafka. message = " + message);
        }

        // TODO: should be configurable
        String topic = "test";

        kafkaProducer.send(new KeyedMessage<String, DataOperation>(topic, message));
    }

}
