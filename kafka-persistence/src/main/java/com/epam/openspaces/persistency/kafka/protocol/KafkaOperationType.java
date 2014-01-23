package com.epam.openspaces.persistency.kafka.protocol;

/**
 * Created by Oleksiy_Dyagilev
 */
public enum KafkaOperationType {
    WRITE, UPDATE, PARTIAL_UPDATE, REMOVE, REMOVE_BY_UID, CHANGE
}
