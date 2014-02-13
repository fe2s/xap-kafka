package com.epam.openspaces.persistency.kafka;

import com.epam.openspaces.persistency.kafka.EmbeddedSpace.Schema;
import com.epam.openspaces.persistency.kafka.protocol.impl.KafkaDataOperationType;
import com.epam.openspaces.persistency.kafka.protocol.impl.KafkaMessage;
import com.gigaspaces.document.SpaceDocument;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.UrlSpaceConfigurer;

import java.util.ArrayList;
import java.util.HashMap;
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
    private static EmbeddedSpace embeddedMirror;
    private static int zookeeperPort;
    private static GigaSpace gigaspace;

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

        embeddedMirror = new EmbeddedSpace("mirror.xml", Schema.NONE);
        embeddedMirror.startup();

        gigaspace = new GigaSpaceConfigurer(new UrlSpaceConfigurer("jini://*/*/space?groups=kafka-test")).gigaSpace();

    }

    @AfterClass
    public static void shutdown() {
        embeddedMirror.shutdown();
        embeddedSpace.shutdown();

        embeddedKafka.shutdown();
        embeddedZookeper.shutdown();
    }

    @Test
    public void testPOJO() throws ExecutionException, InterruptedException {
        Future<List<KafkaMessage>> result = submitCallableTask("data");

        List<KafkaMessage> expectedList = new ArrayList<KafkaMessage>();

        for (int i = 0; i < objectCount / 3; i++) {
            long time = System.currentTimeMillis();

            // Insert person to space
            Person person = new Person(i, "Paul " + Long.toString(time));
            gigaspace.write(person);
            addKafkaMessageToListForPojo(expectedList, KafkaDataOperationType.WRITE, person);

            // Update person in space
            person.setName("Paul " + Long.toString(time));
            gigaspace.write(person);
            addKafkaMessageToListForPojo(expectedList, KafkaDataOperationType.UPDATE, person);

            // Remove person to space
            gigaspace.clear(person);
            addKafkaMessageToListForPojo(expectedList, KafkaDataOperationType.REMOVE, person);
        }

        List<KafkaMessage> actualList = result.get();

        assertEquals(expectedList, actualList);
    }

    private void addKafkaMessageToListForPojo(List<KafkaMessage> list, KafkaDataOperationType type, Person document){
        KafkaMessage messageRemove = new KafkaMessage(type, document);
        list.add(messageRemove);
    }

    @Test
    public void testExtendedSpaceDocument() throws ExecutionException, InterruptedException {
        Future<List<KafkaMessage>> result = submitCallableTask("Product");

        List<KafkaMessage> expectedList = new ArrayList<KafkaMessage>();

        for (int i = 0; i < objectCount / 3; i++) {
            long time = System.currentTimeMillis();
            // Insert product to space
            Product product = new Product()
                    .setCatalogNumber("hw-"+i)
                    .setName("Anvil")
                    .setPrice((float) Math.random() * 100);

            gigaspace.write(product);
            addKafkaMessageToListForSpaceDocument(expectedList, KafkaDataOperationType.WRITE, product);

            // Update product in space
            product.setPrice((float) Math.random() * 100);
            gigaspace.write(product);
            addKafkaMessageToListForSpaceDocument(expectedList, KafkaDataOperationType.UPDATE, product);

            // Remove product from space
            gigaspace.clear(product);
            addKafkaMessageToListForSpaceDocument(expectedList, KafkaDataOperationType.REMOVE, product);

        }

        List<KafkaMessage> actualList = result.get();

        assertEquals(expectedList, actualList);
    }

    @Test
    public void testSpaceDocument() throws ExecutionException, InterruptedException {
        Future<List<KafkaMessage>> result = submitCallableTask("category");

        List<KafkaMessage> expectedList = new ArrayList<KafkaMessage>();

        for (int i = 0; i < objectCount / 3; i++) {
            long time = System.currentTimeMillis();
            // Insert category to space
            SpaceDocument category = new SpaceDocument("Category")
                    .setProperty("name", "category" + i)
                    .setProperty("description", "description")
                    .setProperty(Product.DEFAULT_SPACE_DOCUMENT_KAFKA_TOPIC, "category");

            gigaspace.write(category);
            addKafkaMessageToListForSpaceDocument(expectedList, KafkaDataOperationType.WRITE, category);

            // Update category in space
            category.setProperty("description", "another description");
            gigaspace.write(category);
            addKafkaMessageToListForSpaceDocument(expectedList, KafkaDataOperationType.UPDATE, category);

            // Remove category from space
            gigaspace.clear(category);
            addKafkaMessageToListForSpaceDocument(expectedList, KafkaDataOperationType.REMOVE, category);
        }

        List<KafkaMessage> actualList = result.get();

        assertEquals(expectedList, actualList);
    }

    private void addKafkaMessageToListForSpaceDocument(List<KafkaMessage> list, KafkaDataOperationType type, SpaceDocument document){
        Map<String, Object> updateObjectAsMap = new HashMap(document.getProperties());
        KafkaMessage messageRemove = new KafkaMessage(type, updateObjectAsMap);
        list.add(messageRemove);
    }

    private Future<List<KafkaMessage>> submitCallableTask(String topicName) {
        TestConsumerTask consumer = new TestConsumerTask(topicName, objectCount, zookeeperPort);
        ExecutorService ex = Executors.newCachedThreadPool();

        return ex.submit(consumer);
    }
}
