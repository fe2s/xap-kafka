package com.epam.consumer;

import kafka.consumer.ConsumerIterator;

import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.springframework.beans.factory.InitializingBean;

import com.epam.openspaces.persistency.kafka.consumer.KafkaConsumer;
import com.epam.openspaces.persistency.kafka.protocol.KafkaDataOperation;
import com.gigaspaces.sync.SpaceSynchronizationEndpointException;

public class Consumer implements InitializingBean {

    private KafkaConsumer consumer;

    private SessionFactory sessionFactory;

    public void setConsumer(KafkaConsumer consumer) {
        this.consumer = consumer;
    }

    public void setSessionFactory(SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    @Override
    public void afterPropertiesSet() throws Exception {

        consume();
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

    private SessionFactory getSessionFactory() {
        return sessionFactory;
    }

}
