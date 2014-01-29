package com.epam.common;

import com.epam.openspaces.persistency.kafka.annotations.KafkaTopic;
import com.gigaspaces.annotation.pojo.SpaceClass;
import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceRouting;

import javax.persistence.Entity;
import javax.persistence.Table;
import javax.persistence.Id;
import java.io.Serializable;

/**
 * A simple object used to work with the Space.
 */
@KafkaTopic("data")
@Entity
@Table(name="DATA")
@SpaceClass
public class Data implements Serializable {

    @Id
    private String id;

    private Long type;

    private String rawData;

    /**
     * Constructs a new Data object.
     */
    public Data() {
    }

    /**
     * Constructs a new Data object with the given type
     * and raw data.
     */
    public Data(long type, String rawData) {
        this.type = type;
        this.rawData = rawData;
    }

    /**
     * The id of this object.
     */
    @SpaceId(autoGenerate=true)
    public String getId() {
        return id;
    }

    /**
     * The id of this object. Its value will be auto generated when it is written
     * to the space.
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * The type of the data object. Used as the routing field when working with
     * a partitioned space.
     */
    @SpaceRouting
    public Long getType() {
        return type;
    }

    /**
     * The type of the data object. Used as the routing field when working with
     * a partitioned space.
     */
    public void setType(Long type) {
        this.type = type;
    }

    /**
     * The raw data this object holds.
     */
    public String getRawData() {
        return rawData;
    }

    /**
     * The raw data this object holds.
     */
    public void setRawData(String rawData) {
        this.rawData = rawData;
    }

    @Override
    public String toString() {
        return "Data{" +
                "id='" + id + '\'' +
                ", type=" + type +
                ", rawData='" + rawData + '\'' +
                '}';
    }
}
