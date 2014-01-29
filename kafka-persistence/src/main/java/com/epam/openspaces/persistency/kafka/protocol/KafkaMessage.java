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

    public KafkaMessage(KafkaDataOperationType dataOperationType, Serializable dataAsObject) {
        this.dataOperationType = dataOperationType;
        this.dataAsObject = dataAsObject;
    }

    public KafkaMessage(KafkaDataOperationType dataOperationType, Map<String, Object> dataAsMap) {
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
    public String toString() {
        return "KafkaMessage{" +
                "dataOperationType=" + dataOperationType +
                ", dataAsObject=" + dataAsObject +
                ", dataAsMap=" + dataAsMap +
                '}';
    }
}
