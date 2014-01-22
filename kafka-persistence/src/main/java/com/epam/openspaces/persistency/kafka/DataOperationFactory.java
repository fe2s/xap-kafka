package com.epam.openspaces.persistency.kafka;

import com.epam.openspaces.persistency.kafka.protocol.DataOperation;
import com.epam.openspaces.persistency.kafka.protocol.OperationType;
import com.gigaspaces.sync.DataSyncOperation;
import com.gigaspaces.sync.DataSyncOperationType;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Oleksiy_Dyagilev
 */
public class DataOperationFactory {

    private static final Map<DataSyncOperationType, OperationType> typesMap = new HashMap<DataSyncOperationType, OperationType>();
    static {
        typesMap.put(DataSyncOperationType.WRITE, OperationType.WRITE);
        typesMap.put(DataSyncOperationType.CHANGE, OperationType.CHANGE);
        typesMap.put(DataSyncOperationType.PARTIAL_UPDATE, OperationType.PARTIAL_UPDATE);
        typesMap.put(DataSyncOperationType.REMOVE, OperationType.REMOVE);
        typesMap.put(DataSyncOperationType.REMOVE_BY_UID, OperationType.REMOVE_BY_UID);
        typesMap.put(DataSyncOperationType.UPDATE, OperationType.UPDATE);
    }

    public static DataOperation create(DataSyncOperation syncOperation) throws KafkaPersistenceException {
        OperationType type = typesMap.get(syncOperation.getDataSyncOperationType());

        if (syncOperation.supportsDataAsDocument()) {
            return new DataOperation(type, syncOperation.getDataAsDocument().getProperties());
        } else if (syncOperation.supportsDataAsObject()) {
            Object object = syncOperation.getDataAsObject();
            if (object instanceof Serializable) {
                return new DataOperation(type, (Serializable) object);
            } else {
                throw new KafkaPersistenceException("Not serializable object of DataSyncOperation " + syncOperation);
            }
        } else {
            throw new KafkaPersistenceException("Unable to convert DataSyncOperation to Kafka protocol. " +
                    "DataSyncOperation = " + syncOperation);
        }
    }

}
