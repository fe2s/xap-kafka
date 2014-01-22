package com.epam;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;
import org.apache.commons.lang.SerializationUtils;

import java.io.Serializable;

/**
 * Created by Oleksiy_Dyagilev
 */
public class MyEncoder implements Encoder {

    public MyEncoder(VerifiableProperties props) {
        // TODO ?
    }

    @Override
    public byte[] toBytes(Object o) {
        if (o instanceof Serializable) {
            return SerializationUtils.serialize((Serializable)o);
        } else {
            throw new RuntimeException("Object " + o + " is not serializable");
        }
    }

}
