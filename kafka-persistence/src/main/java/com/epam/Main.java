package com.epam;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

/**
 * Created by Oleksiy_Dyagilev
 */
public class Main {
    public static void main(String[] args) {

//        long events = 5;
//        Random rnd = new Random();
//
//        Properties props = new Properties();
//        props.put("metadata.broker.list", "localhost:9092");
//        props.put("serializer.class", "com.epam.openspaces.persistency.kafka.serializer.KafkaDataOperationEncoder");
//        props.put("partitioner.class", "kafka.producer.DefaultPartitioner");
//        props.put("request.required.acks", "1");
//
//        ProducerConfig config = new ProducerConfig(props);

//        Producer<String, String> producer = new Producer<String, String>(config);
//
//        for (long nEvents = 0; nEvents < events; nEvents++) {
//            long runtime = new Date().getTime();
//            String ip = "192.168.2." + rnd.nextInt(255);
//            String msg = runtime + ",www.example.com," + ip;
//            KeyedMessage<String, String> data = new KeyedMessage<String, String>("test", ip, msg);
//            producer.send(data);
//        }
//        producer.close();

//        Producer<String, Person> producer = new Producer<String, Person>(config);
//
//        for (long nEvents = 0; nEvents < events; nEvents++) {
//            long runtime = new Date().getTime();
//            Person person = new Person("aaa" + runtime);
//            KeyedMessage<String, Person> data = new KeyedMessage<String, Person>("test", person);
//            producer.send(data);
//        }
//        producer.close();
    }

}
