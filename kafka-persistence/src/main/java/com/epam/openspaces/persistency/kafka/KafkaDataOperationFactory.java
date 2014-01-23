package com.epam.openspaces.persistency.kafka;

import com.epam.openspaces.persistency.kafka.protocol.KafkaDataOperation;
import com.epam.openspaces.persistency.kafka.protocol.KafkaOperationType;
import com.gigaspaces.sync.DataSyncOperation;
import com.gigaspaces.sync.DataSyncOperationType;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Oleksiy_Dyagilev
 */
public class KafkaDataOperationFactory {

    private static final Map<DataSyncOperationType, KafkaOperationType> typesMap = new HashMap<DataSyncOperationType, KafkaOperationType>();
    static {
        typesMap.put(DataSyncOperationType.WRITE, KafkaOperationType.WRITE);
        typesMap.put(DataSyncOperationType.CHANGE, KafkaOperationType.CHANGE);
        typesMap.put(DataSyncOperationType.PARTIAL_UPDATE, KafkaOperationType.PARTIAL_UPDATE);
        typesMap.put(DataSyncOperationType.REMOVE, KafkaOperationType.REMOVE);
        typesMap.put(DataSyncOperationType.REMOVE_BY_UID, KafkaOperationType.REMOVE_BY_UID);
        typesMap.put(DataSyncOperationType.UPDATE, KafkaOperationType.UPDATE);
    }

    public static KafkaDataOperation create(DataSyncOperation syncOperation) throws KafkaPersistenceException {
        KafkaOperationType type = typesMap.get(syncOperation.getDataSyncOperationType());

        if (syncOperation.supportsDataAsObject()) {
            Object object = syncOperation.getDataAsObject();
            if (object instanceof Serializable) {
                return new KafkaDataOperation(type, (Serializable) object);
            } else {
                throw new KafkaPersistenceException("Not serializable object of DataSyncOperation " + syncOperation);
            }
        } else if (syncOperation.supportsDataAsDocument()) {
            return new KafkaDataOperation(type, syncOperation.getDataAsDocument().getProperties());
        } else {
            throw new KafkaPersistenceException("Unable to convert DataSyncOperation to Kafka protocol. " +
                    "DataSyncOperation = " + syncOperation);
        }
    }

}
