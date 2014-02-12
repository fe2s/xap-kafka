package com.epam.openspaces.persistency.kafka;

import com.epam.openspaces.persistency.kafka.EmbeddedSpace.Schema;
import com.epam.openspaces.persistency.kafka.protocol.impl.KafkaMessage;
import com.epam.openspaces.persistency.kafka.protocol.impl.KafkaDataOperationType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.UrlSpaceConfigurer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;

public class KafkaPersistenceTest {

    private static final int objectCount = 30;
    private static EmbeddedZookeper embeddedZookeper;
    private static EmbeddedKafka embeddedKafka;
    private static EmbeddedSpace embeddedSpace;
    private static EmbeddedSpace embeddedMiror;
    private static int zookeeperPort;

    @BeforeClass
    public static void init() throws Exception {
        ProducerProperties props = ProducerProperties.getInstance();

        zookeeperPort = props.getZookeeperPort();
        int kafkaPort = props.getKafkaPort();

        embeddedZookeper = new EmbeddedZookeper(zookeeperPort);
        embeddedZookeper.startup();

        embeddedKafka = new EmbeddedKafka(kafkaPort, zookeeperPort);
        embeddedKafka.startup();

        embeddedSpace = new EmbeddedSpace("space.xml", Schema.PARTITIONED);
        embeddedSpace.startup();

        embeddedMiror = new EmbeddedSpace("mirror.xml", Schema.NONE);
        embeddedMiror.startup();

    }

    @AfterClass
    public static void shutdown() {
        embeddedMiror.shutdown();
        embeddedSpace.shutdown();

        embeddedKafka.shutdown();
        embeddedZookeper.shutdown();
    }

    @Test
    public void shouldPassWhenProducePOJO() throws ExecutionException, InterruptedException {

        GigaSpace gigaspace = new GigaSpaceConfigurer(new UrlSpaceConfigurer("jini://*/*/space?groups=kafka-test")).gigaSpace();

        TestConsumerTask consumer = new TestConsumerTask("data", objectCount, zookeeperPort);
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

    @Test
    public void shouldPassWhenProduceSpaceDocument() throws ExecutionException, InterruptedException {

        GigaSpace gigaspace = new GigaSpaceConfigurer(new UrlSpaceConfigurer("jini://*/*/space?groups=kafka-test")).gigaSpace();

        TestConsumerTask consumer = new TestConsumerTask("Product", objectCount, zookeeperPort);
        ExecutorService ex = Executors.newCachedThreadPool();

        Future<List<KafkaMessage>> result = ex.submit(consumer);

        List<KafkaMessage> expectedList = new ArrayList<KafkaMessage>();

        for (int i = 0; i < objectCount / 3; i++) {
            long time = System.currentTimeMillis();
            // Insert product to space
            Product product = new Product()
                    .setCatalogNumber("hw-"+i)
                    .setName("Anvil")
                    .setPrice(9.99f);

            gigaspace.write(product);
            Map<String, Object> writeObjectAsMap = product.getProperties();
            KafkaMessage messageWrite = new KafkaMessage(KafkaDataOperationType.WRITE, writeObjectAsMap);
            expectedList.add(messageWrite);

            product.setPrice(9.99f);
            gigaspace.write(product);
            Map<String, Object> updateObjectAsMap = product.getProperties();
            KafkaMessage messageUpdate = new KafkaMessage(KafkaDataOperationType.UPDATE, updateObjectAsMap);
            expectedList.add(messageUpdate);

            // Remove product to space
            gigaspace.clear(product);
            Map<String, Object> removeObjectAsMap = product.getProperties();
            KafkaMessage messageRemove = new KafkaMessage(KafkaDataOperationType.REMOVE, removeObjectAsMap);
            expectedList.add(messageRemove);

        }

        List<KafkaMessage> actualList = result.get();

        assertEquals(expectedList, actualList);
    }
}
