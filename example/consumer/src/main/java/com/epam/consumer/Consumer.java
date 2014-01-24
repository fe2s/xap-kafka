package com.epam.consumer;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.epam.openspaces.persistency.kafka.protocol.KafkaMessage;
import kafka.consumer.ConsumerIterator;

import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.springframework.beans.factory.InitializingBean;

import com.epam.openspaces.persistency.kafka.consumer.KafkaConsumer;
import com.gigaspaces.sync.SpaceSynchronizationEndpointException;

public class Consumer implements InitializingBean {

    private KafkaConsumer consumer;
    private ScheduledExecutorService executorService;
    private SessionFactory sessionFactory;

    public void setConsumer(KafkaConsumer consumer) {
        this.consumer = consumer;
    }

    public void setSessionFactory(SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    @Override
    public void afterPropertiesSet() throws Exception {

        executorService = Executors.newScheduledThreadPool(1);
        executorService.execute(new ConsumerTask());
    }

    private void consume() {

        ConsumerIterator<String, KafkaMessage> iterator = consumer
                .getKafkaIterator("data");

        while (iterator.hasNext()) {
            Session session = getSessionFactory().openSession();
            Transaction tr = session.beginTransaction();
            try {

                KafkaMessage kafkaMessage = iterator.next().message();

                switch (kafkaMessage.getDataOperationType()) {
                    case WRITE:
                        executeWrite(session, kafkaMessage);
                        break;
                    default:
                        break;
                }
                tr.commit();
            } catch (Exception e) {
                tr.rollback();
                throw new SpaceSynchronizationEndpointException(
                        "Failed to execute bulk operation, latest object", e);
            } finally {
                if (session.isOpen()) {
                    session.close();
                }
            }
        }

    }

    private void executeWrite(Session session, KafkaMessage kafkaMessage) {
        if (!kafkaMessage.hasDataAsObject()) {
            return;
        }

        Object entry = kafkaMessage.getDataAsObject();

        try {
            session.saveOrUpdate(entry);
        } catch (HibernateException e) {
            session.merge(entry);
        }
    }

    private SessionFactory getSessionFactory() {
        return sessionFactory;
    }

    public class ConsumerTask implements Runnable {

        @Override
        public void run() {
            consume();
        }

    }

}
