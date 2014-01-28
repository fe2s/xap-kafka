package com.epam.openspaces.persistency.kafka;

import java.io.File;

/**
 * Created by Oleksiy_Dyagilev
 */
public class TestUtils {

    public static File tempDir() {
        String ioDir = System.getProperty("java.io.tmpdir");
        File f = new File(ioDir, "kafka-" + (int) (Math.random() * 1000000));
        f.mkdirs();
        f.deleteOnExit();
        return f;
    }

}
