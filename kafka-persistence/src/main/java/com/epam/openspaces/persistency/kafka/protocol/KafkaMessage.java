package com.epam.openspaces.persistency.kafka.protocol;

import java.io.Serializable;
import java.util.Map;

/**
 * Defines XAP-Kafka message protocol.
 * 
 * @see com.epam.openspaces.persistency.kafka.serializer.KafkaMessageEncoder
 * @see com.epam.openspaces.persistency.kafka.serializer.KafkaMessageDecoder
 * 
 * @author Oleksiy_Dyagilev
 */
public class KafkaMessage implements Serializable {

    private KafkaDataOperationType dataOperationType;

    private Serializable dataAsObject;
    private Map<String, Object> dataAsMap;

    public KafkaMessage(KafkaDataOperationType dataOperationType,
            Serializable dataAsObject) {
        this.dataOperationType = dataOperationType;
        this.dataAsObject = dataAsObject;
    }

    public KafkaMessage(KafkaDataOperationType dataOperationType,
            Map<String, Object> dataAsMap) {
        this.dataOperationType = dataOperationType;
        this.dataAsMap = dataAsMap;
    }

    public KafkaDataOperationType getDataOperationType() {
        return dataOperationType;
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
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((dataAsMap == null) ? 0 : dataAsMap.hashCode());
        result = prime * result
                + ((dataAsObject == null) ? 0 : dataAsObject.hashCode());
        result = prime
                * result
                + ((dataOperationType == null) ? 0 : dataOperationType
                        .hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        KafkaMessage other = (KafkaMessage) obj;
        if (dataAsMap == null) {
            if (other.dataAsMap != null) {
                return false;
            }
        } else if (!dataAsMap.equals(other.dataAsMap)) {
            return false;
        }
        if (dataAsObject == null) {
            if (other.dataAsObject != null) {
                return false;
            }
        } else if (!dataAsObject.equals(other.dataAsObject)) {
            return false;
        }
        if (dataOperationType != other.dataOperationType) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "KafkaMessage{" + "dataOperationType=" + dataOperationType
                + ", dataAsObject=" + dataAsObject + ", dataAsMap=" + dataAsMap
                + '}';
    }
}
