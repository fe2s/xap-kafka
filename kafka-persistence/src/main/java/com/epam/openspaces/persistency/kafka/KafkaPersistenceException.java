package com.epam.openspaces.persistency.kafka;

/**
 * Created by Oleksiy_Dyagilev
 */
public class KafkaPersistenceException extends Exception {

    public KafkaPersistenceException() {
        super();
    }

    public KafkaPersistenceException(String message) {
        super(message);
    }

    public KafkaPersistenceException(String message, Throwable cause) {
        super(message, cause);
    }

    public KafkaPersistenceException(Throwable cause) {
        super(cause);
    }

    protected KafkaPersistenceException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}