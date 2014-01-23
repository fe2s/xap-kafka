package com.epam.openspaces.persistency.kafka.protocol;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by Oleksiy_Dyagilev
 */
public class KafkaDataOperation implements Serializable {

    private KafkaOperationType type;

    // TODO: what about delete by id ?

    private Serializable dataAsObject;
    private Map<String, Object> dataAsMap;

    public KafkaDataOperation(KafkaOperationType type, Serializable dataAsObject) {
        this.type = type;
        this.dataAsObject = dataAsObject;
    }

    public KafkaDataOperation(KafkaOperationType type, Map<String, Object> dataAsMap) {
        this.type = type;
        this.dataAsMap = dataAsMap;
    }

    public KafkaOperationType getType() {
        return type;
    }

    public Serializable getDataAsObject() {
        return dataAsObject;
    }

    public Map<String, Object> getDataAsMap() {
        return dataAsMap;
    }

    public boolean hasDataAsObject() {
        return dataAsObject != null;
    }

    public boolean hasDataAsMap() {
        return dataAsMap != null;
    }

    @Override
    public String toString() {
        return "KafkaDataOperation{" +
                "type=" + type +
                ", dataAsObject=" + dataAsObject +
                ", dataAsMap=" + dataAsMap +
                '}';
    }
}
