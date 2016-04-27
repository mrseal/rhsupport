package com.cf.test.jms;

import javax.annotation.Resource;
import javax.ejb.Singleton;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.jms.*;

import org.apache.commons.lang.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class EventSender {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource(name = "java:/JmsXA")
    private ConnectionFactory connectionFactory;

    @Resource(name = "java:/queue/testqueue")
    private Queue queue;

    @TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
    public void send(final long num, final int size) {
        final String text = createMessage(size);
        for (int i = 0; i < num; i++) {
            final boolean sent = send(text);
            if (!sent) {
                break;
            }
        }
    }

    private boolean send(final String text) {
        try {
            final Connection connection = connectionFactory.createConnection();
            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final MessageProducer producer = session.createProducer(queue);
            final Message message = session.createTextMessage(text);

            // Delivery Mode: NON_PERSISTENT or PERSISTENT
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

            producer.send(message);
            producer.close();
            session.close();
            connection.close();
            logger.info("====== Successfully sent message {}", message);
            return true;
        } catch (final Exception e) {
            logger.error("====== Failed to send message", e);
            return false;
        }
    }

    private String createMessage(final int size) {
        return RandomStringUtils.randomAscii(size);
    }

}
