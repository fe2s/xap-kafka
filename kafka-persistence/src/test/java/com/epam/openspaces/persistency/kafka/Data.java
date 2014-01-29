package com.epam.openspaces.persistency.kafka;

import java.io.Serializable;

import com.epam.openspaces.persistency.kafka.annotations.KafkaTopic;
import com.gigaspaces.annotation.pojo.SpaceClass;
import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceRouting;

/**
 * A simple object used to work with the Space.
 */
@KafkaTopic("data")
@SpaceClass
public class Data implements Serializable {

    private String id;

    private Long type;

    private String rawData;

    /**
     * Constructs a new Data object.
     */
    public Data() {
    }

    /**
     * Constructs a new Data object with the given type and raw data.
     */
    public Data(long type, String rawData) {
        this.type = type;
        this.rawData = rawData;
    }

    /**
     * The id of this object.
     */
    @SpaceId(autoGenerate = true)
    public String getId() {
        return id;
    }

    /**
     * The id of this object. Its value will be auto generated when it is
     * written to the space.
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
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
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
        Data other = (Data) obj;
        if (id == null) {
            if (other.id != null) {
                return false;
            }
        } else if (!id.equals(other.id)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "Data{" + "id='" + id + '\'' + ", type=" + type + ", rawData='"
                + rawData + '\'' + '}';
    }
}
