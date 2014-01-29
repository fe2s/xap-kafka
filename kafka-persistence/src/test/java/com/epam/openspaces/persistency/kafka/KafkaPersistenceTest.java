package com.epam.openspaces.persistency.kafka;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.UrlSpaceConfigurer;

import com.epam.openspaces.persistency.kafka.EmbeddedSpace.Schema;

public class KafkaPersistenceTest {

    private static EmbeddedZookeper embeddedZookeper;
    private static EmbeddedKafka embeddedKafka;
    private static EmbeddedSpace embeddedSpace;
    private static EmbeddedSpace embeddedMiror;

    @BeforeClass
    public static void init() throws Exception {
        int zookeeperPort = 2181; // TODO: we might want to choose from
                                  // available ones instead of hardcoding
        int kafkaPort = 9092;

        embeddedZookeper = new EmbeddedZookeper(zookeeperPort);
        embeddedZookeper.startup();

        embeddedKafka = new EmbeddedKafka(kafkaPort, zookeeperPort);
        embeddedKafka.startup();

        embeddedSpace = new EmbeddedSpace("space.xml", Schema.PARTITIONED);
        embeddedSpace.startup();

        embeddedMiror = new EmbeddedSpace("mirror.xml", Schema.NONE);
        embeddedMiror.startup();
    }

    @Test
    public void test() throws InterruptedException {
        System.out.println("test");

        GigaSpace gigaspace = new GigaSpaceConfigurer(new UrlSpaceConfigurer(
                "jini://*/*/space?groups=kafka-test")).gigaSpace();

        int objectCount = 30;

        TestConsumerTask consumer = new TestConsumerTask("data", objectCount);
        consumer.start();

        for (int i = 0; i < objectCount; i++) {
            long time = System.currentTimeMillis();
            Data data = new Data(i, "FEEDER " + Long.toString(time));
            gigaspace.write(data);
        }

        consumer.join();
    }

    @AfterClass
    public static void shutdown() {
        embeddedKafka.shutdown();
        embeddedZookeper.shutdown();
        embeddedMiror.shutdown();
        embeddedSpace.shutdown();
    }
}
