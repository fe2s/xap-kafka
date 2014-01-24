package com.epam.consumer;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import kafka.common.KafkaException;
import kafka.consumer.ConsumerIterator;

import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.springframework.beans.factory.InitializingBean;

import com.epam.openspaces.persistency.kafka.consumer.KafkaConsumer;
import com.epam.openspaces.persistency.kafka.protocol.KafkaDataOperation;

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

        ConsumerIterator<String, KafkaDataOperation> iterator = consumer
                .getKafkaIterator("data");

        while (iterator.hasNext()) {
            Session session = getSessionFactory().openSession();
            Transaction tr = session.beginTransaction();
            try {

                KafkaDataOperation dataOperation = iterator.next().message();

                switch (dataOperation.getType()) {
                case WRITE:
                    executeWrite(session, dataOperation);
                    break;
                case UPDATE:
                    executeUpdate(session, dataOperation);
                    break;
                case REMOVE:
                    executeRemove(session, dataOperation);
                default:
                    break;
                }
                tr.commit();
            } catch (Exception e) {
                tr.rollback();
                throw new KafkaException(
                        "Failed to execute bulk operation, latest object", e);
            } finally {
                if (session.isOpen()) {
                    session.close();
                }
            }
        }

    }

    private void executeRemove(Session session, KafkaDataOperation dataOperation) {
        if (!dataOperation.hasDataAsObject()) {
            return;
        }

        Object entry = dataOperation.getDataAsObject();

        try {
            session.delete(entry);
        } catch (HibernateException e) {
            session.delete(session.merge(entry));
        }
    }

    private void executeWrite(Session session, KafkaDataOperation dataOperation) {
        if (!dataOperation.hasDataAsObject()) {
            return;
        }

        Object entry = dataOperation.getDataAsObject();

        try {
            session.saveOrUpdate(entry);
        } catch (HibernateException e) {
            session.merge(entry);
        }
    }

    private void executeUpdate(Session session, KafkaDataOperation dataOperation) {
        if (!dataOperation.hasDataAsObject()) {
            return;
        }

        Object entry = dataOperation.getDataAsObject();

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
