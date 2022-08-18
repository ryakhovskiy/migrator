package org.kr.db.migrator.writer;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import javax.jms.*;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * Created with IntelliJ IDEA.
 * User: I005144
 * Date: 01.07.13
 * Time: 13:32
 * To change this template use File | Settings | File Templates.
 */
public class JmsReader implements Runnable {

    private final static long TIMEOUT_MS = 1000;
    private final Logger log = Logger.getLogger(JmsReader.class);
    private final String brokerUrl;
    private final String queueTopic;
    private final BlockingQueue<List> queue;
    private final ObjectMapper jsonParser = new ObjectMapper();
    private Connection jmsConnection;
    private Session jmsSession;
    private MessageConsumer jmsMessageConsumer;

    public JmsReader(String brokerUrl, String queueTopic, BlockingQueue<List> queue) {
        this.brokerUrl = brokerUrl;
        this.queueTopic = queueTopic;
        this.queue = queue;
    }

    private void prepareJms() throws JMSException {
        ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory(brokerUrl);
        jmsConnection = activeMQConnectionFactory.createConnection();
        jmsConnection.start();
        jmsSession = jmsConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = jmsSession.createQueue(queueTopic);
        jmsMessageConsumer = jmsSession.createConsumer(destination);
        log.debug("JmsReader started...");
    }

    @Override
    public void run() {
        try {
            prepareJms();
            doJmsReaderJob();
        } catch (JMSException e) {
            log.error(e);
        } catch (InterruptedException e) {
            log.debug("JmsSender has been interrupted");
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            dispose();
        }
    }

    private void doJmsReaderJob() throws JMSException, InterruptedException {
        while (true) {
            Message message = jmsMessageConsumer.receive(TIMEOUT_MS);
            if (Thread.currentThread().isInterrupted())
                throw new InterruptedException();
            if (log.isTraceEnabled())
                log.trace("Message received: " + message);
            if (!(message instanceof BytesMessage)) {
                if (log.isTraceEnabled())
                    log.trace("Non-bytes message recevied: " + message);
                continue;
            }
            BytesMessage bytesMessage = (BytesMessage)message;
            int size = bytesMessage.getIntProperty("size");
            byte[] b = new byte[size];
            bytesMessage.readBytes(b);
            String text = new String(b, Charset.forName("UTF-8"));
            if (log.isTraceEnabled())
                log.trace("Text message: " + text);
            sendMessageToSqlWriter(text);
        }
    }

    private void sendMessageToSqlWriter(String text) throws InterruptedException {
        try {
            List list = jsonParser.readValue(text, List.class);
            if (log.isTraceEnabled())
                log.trace("putting message to SqlWriter queue");
            queue.put(list);
        } catch (IOException e) {
            log.error("Error while parsing JSON content", e);
        }
    }

    private void dispose() {
        try {
            jmsMessageConsumer.close();
        } catch (JMSException e) {
            log.error("Error while closing JmsMessageConsumer...");
        }
        try {
            jmsSession.close();
        } catch (JMSException e) {
            log.error("Error while closing JmsSession");
        }
        try {
            jmsConnection.close();
        } catch (JMSException e) {
            log.error("Error while closing JmsConnection");
        }
    }
}
