package com.epam.openspaces.persistency.kafka.protocol;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by Oleksiy_Dyagilev
 */
public class DataOperation implements Serializable {

    private OperationType type;

    // TODO: what about delete by id ?

    private Serializable dataAsObject;
    private Map<String, Object> dataAsMap;

    public DataOperation(OperationType type, Serializable dataAsObject) {
        this.type = type;
        this.dataAsObject = dataAsObject;
    }

    public DataOperation(OperationType type, Map<String, Object> dataAsMap) {
        this.type = type;
        this.dataAsMap = dataAsMap;
    }

    public OperationType getType() {
        return type;
    }

    public Serializable getDataAsObject() {
        return dataAsObject;
    }

    public Map<String, Object> getDataAsMap() {
        return dataAsMap;
    }

    @Override
    public String toString() {
        return "DataOperation{" +
                "type=" + type +
                ", dataAsObject=" + dataAsObject +
                ", dataAsMap=" + dataAsMap +
                '}';
    }
}
