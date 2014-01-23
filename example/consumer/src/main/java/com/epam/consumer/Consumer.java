package com.epam.consumer;

import kafka.consumer.ConsumerIterator;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import com.epam.openspaces.persistency.kafka.consumer.KafkaConsumer;
import com.epam.openspaces.persistency.kafka.protocol.KafkaDataOperation;

public class Consumer implements InitializingBean, DisposableBean {

    private KafkaConsumer consumer;

    public void setConsumer(KafkaConsumer consumer) {
        this.consumer = consumer;
    }

    @Override
    public void destroy() throws Exception {

    }

    @Override
    public void afterPropertiesSet() throws Exception {
        ConsumerIterator<String, KafkaDataOperation> iterator = consumer
                .getKafkaIterator("data");

        while (iterator.hasNext()) {
            System.out.println(iterator.next().message());
        }
    }

}
