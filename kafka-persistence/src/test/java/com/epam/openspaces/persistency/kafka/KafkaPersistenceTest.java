package com.epam.openspaces.persistency.kafka;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Properties;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;

import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class KafkaPersistenceTest {

    private static NIOServerCnxn.Factory standaloneServerFactory;
    private static KafkaServer kafkaServer;

    @BeforeClass
    public static void init() throws IOException, InterruptedException {

        int clientPort = 2181;
        int tickTime = 2000;
        String dataDirectory = System.getProperty("java.io.tmpdir");

        File dir = new File(dataDirectory, "zookeeper").getAbsoluteFile();

        ZooKeeperServer server = new ZooKeeperServer(dir, dir, tickTime);
        standaloneServerFactory = new NIOServerCnxn.Factory(
                new InetSocketAddress(clientPort), 1);

        standaloneServerFactory.startup(server);

        Properties props = new Properties();
        props.setProperty("host.name", "localhost");
        props.setProperty("zookeeper.connect", "localhost:2181");
        props.setProperty("port", "9092");
        props.setProperty("broker.id", "0");
        props.setProperty("num.partitions", "1");
        props.setProperty("log.dir", dir.getAbsolutePath());

        kafkaServer = new KafkaServer(new KafkaConfig(props), null);
        kafkaServer.startup();

    }

    @Test
    public void test() throws InterruptedException {

        Producer producerThread = new Producer(KafkaProperties.topic);
        producerThread.start();

        Consumer consumerThread = new Consumer(KafkaProperties.topic);
        consumerThread.start();

        while (true) {
        }
    }

    @AfterClass
    public static void shutdown() {

        standaloneServerFactory.shutdown();
        kafkaServer.shutdown();

    }
}
