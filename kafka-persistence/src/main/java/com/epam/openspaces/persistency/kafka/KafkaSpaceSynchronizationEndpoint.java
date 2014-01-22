package com.epam.openspaces.persistency.kafka;

import com.gigaspaces.sync.DataSyncOperation;
import com.gigaspaces.sync.OperationsBatchData;
import com.gigaspaces.sync.SpaceSynchronizationEndpoint;
import com.gigaspaces.sync.TransactionData;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by Oleksiy_Dyagilev
 */
public class KafkaSpaceSynchronizationEndpoint extends SpaceSynchronizationEndpoint {

    protected static final Log logger = LogFactory.getLog(KafkaSpaceSynchronizationEndpoint.class);

    @Override
    public void onTransactionSynchronization(TransactionData transactionData) {
        executeDataSyncOperations(transactionData.getTransactionParticipantDataItems());
    }

    @Override
    public void onOperationsBatchSynchronization(OperationsBatchData batchData) {
        executeDataSyncOperations(batchData.getBatchDataItems());
    }

    private void executeDataSyncOperations(DataSyncOperation[] transactionParticipantDataItems) {
        for (DataSyncOperation dataSyncOperation : transactionParticipantDataItems) {
            logger.info("dataSyncOperation = " + dataSyncOperation);
            logger.info("object = " + dataSyncOperation.getDataAsObject());
        }
    }

}
