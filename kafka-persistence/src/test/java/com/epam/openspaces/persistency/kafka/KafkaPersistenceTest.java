package com.epam.openspaces.persistency.kafka;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Properties;

import kafka.Kafka;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;

import kafka.server.KafkaServerStartable;
import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class KafkaPersistenceTest {

    private static EmbeddedZookeper embeddedZookeper;
    private static EmbeddedKafka embeddedKafka;

    @BeforeClass
    public static void init() throws Exception {
        int zookeeperPort = 2181; // TODO: we might want to choose from available ones instead of hardcoding
        int kafkaPort = 9092;

        embeddedZookeper = new EmbeddedZookeper(zookeeperPort);
        embeddedZookeper.startup();

        embeddedKafka = new EmbeddedKafka(kafkaPort, zookeeperPort);
        embeddedKafka.startup();
    }

    @Test
    public void test() throws InterruptedException {
        System.out.println("test");

//        Producer producerThread = new Producer(KafkaProperties.topic);
//        producerThread.start();
//
//        Consumer consumerThread = new Consumer(KafkaProperties.topic);
//        consumerThread.start();
//
//        while (true) {
//        }
    }

    @AfterClass
    public static void shutdown() {
        embeddedKafka.shutdown();
        embeddedZookeper.shutdown();
    }
}
