package com.epam.openspaces.persistency.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.UrlSpaceConfigurer;

import static org.junit.Assert.assertEquals;

import com.epam.openspaces.persistency.kafka.EmbeddedSpace.Schema;
import com.epam.openspaces.persistency.kafka.protocol.KafkaDataOperationType;
import com.epam.openspaces.persistency.kafka.protocol.KafkaMessage;

public class KafkaPersistenceTest {

    private static EmbeddedZookeper embeddedZookeper;
    private static EmbeddedKafka embeddedKafka;
    private static EmbeddedSpace embeddedSpace;
    private static EmbeddedSpace embeddedMiror;
    private static final int objectCount = 30;

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
    public void test() throws ExecutionException, InterruptedException {

        System.out.println("test");

        GigaSpace gigaspace = new GigaSpaceConfigurer(new UrlSpaceConfigurer(

        "jini://*/*/space?groups=kafka-test")).gigaSpace();

        TestConsumerTask consumer = new TestConsumerTask("data", objectCount);
        ExecutorService ex = Executors.newCachedThreadPool();

        Future<List<KafkaMessage>> result = ex.submit(consumer);

        List<KafkaMessage> expectedList = new ArrayList<KafkaMessage>();

        for (int i = 0; i < objectCount / 3; i++) {
            long time = System.currentTimeMillis();
            // Insert data to space
            Data data = new Data(i, "FEEDER Write" + Long.toString(time));
            gigaspace.write(data);
            KafkaMessage messageWrite = new KafkaMessage(KafkaDataOperationType.WRITE, data);
            expectedList.add(messageWrite);

            // Update data to space
            data.setRawData("FEEDER Update" + Long.toString(time));
            gigaspace.write(data);
            KafkaMessage messageUpdate = new KafkaMessage(KafkaDataOperationType.UPDATE, data);
            expectedList.add(messageUpdate);

            // Remove data to space
            gigaspace.clear(data);
            KafkaMessage messageRemove = new KafkaMessage(KafkaDataOperationType.REMOVE, data);
            expectedList.add(messageRemove);

        }

        List<KafkaMessage> actualList = result.get();

        assertEquals(expectedList, actualList);

    }

    @AfterClass
    public static void shutdown() {
        embeddedKafka.shutdown();
        embeddedZookeper.shutdown();

        embeddedMiror.shutdown();
        embeddedSpace.shutdown();

    }
}
