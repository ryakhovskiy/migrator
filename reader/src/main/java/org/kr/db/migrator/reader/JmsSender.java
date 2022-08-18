package org.kr.db.migrator.reader;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * Created with IntelliJ IDEA.
 * User: I005144
 * Date: 28.06.13
 * Time: 18:16
 * To change this template use File | Settings | File Templates.
 */
public class JmsSender extends Thread {

    private final Logger log = Logger.getLogger(JmsSender.class);
    private final String brokerUrl;
    private final String queueTopic;
    private final BlockingQueue<List> queue;
    private final ObjectWriter jsonWriter = new ObjectMapper().writer();
    private Connection jmsConnection;
    private Session jmsSession;
    private MessageProducer jmsMessageProducer;
    private volatile boolean needToWait = false;
    private final Object browserWaiter = new Object();
    private volatile boolean needToStop = false;
    private long messageCounter;

    public JmsSender(String brokerUrl, String queueTopic, BlockingQueue<List> queue, String name) {
        super("JmsSender " + name);
        this.queue = queue;
        this.brokerUrl = brokerUrl;
        this.queueTopic = queueTopic;
        log.info("JmsSender created");
    }

    @Override
    public void run() {
        try {
            doJmsSenderJob();
        } catch (JMSException e) {
            log.error(e);
        } catch (InterruptedException e) {
            log.debug("JmsSender has been interrupted");
        }
    }

    public synchronized void suggestToStop() {
        this.needToStop = true;
    }

    private void doJmsSenderJob() throws JMSException, InterruptedException {
        try {
            prepareJms();
            log.info("starting listening internal queue and sending messages to broker");
            while (true) {
                listenQueueAndSendMessageToJms();
                if (Thread.currentThread().isInterrupted())
                    throw new InterruptedException();
            }
        } finally {
            log.info("Closing session...");
            if (null != jmsSession)
                jmsSession.close();
            log.info("Closing connection...");
            if (null != jmsConnection)
                jmsConnection.close();
            log.info("Total count messages sent by this JmsSender: " + messageCounter);
        }
    }

    private void prepareJms() throws JMSException {
        ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory(brokerUrl);
        jmsConnection = activeMQConnectionFactory.createConnection();
        jmsConnection.start();
        log.info("JmsSender established connection with broker");
        jmsSession = jmsConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = jmsSession.createQueue(queueTopic);
        jmsMessageProducer = jmsSession.createProducer(destination);
        log.info("JmsSender established connection with broker queue");
    }

    private void listenQueueAndSendMessageToJms() throws InterruptedException {
        List list = queue.take();
        sendMessage(list);
        if (needToWait)
            synchronized (browserWaiter) {
                browserWaiter.wait();
            }
    }

    private void sendMessage(List results) {
        try {
            String message = jsonWriter.writeValueAsString(results);
            BytesMessage byteMessage = jmsSession.createBytesMessage();
            byte[] bytes = message.getBytes(Charset.forName("UTF-8"));
            byteMessage.writeBytes(bytes);
            byteMessage.setIntProperty("size", bytes.length);
            if (log.isTraceEnabled())
                log.trace("sending message to JMS: " + message);
            jmsMessageProducer.send(byteMessage);
            handleNewMessageSentEvent();
        } catch (IOException e) {
            log.error("Error while serializing message to json", e);
            log.error(results);
        } catch (JMSException e) {
            log.error("Error while sending message to jms", e);
        }
    }

    private void handleNewMessageSentEvent() {
        if (log.isTraceEnabled())
            log.trace("Internal queue size: " + queue.size());
        if (++messageCounter % 1000 == 0)
            log.info("Total messages sent by this JmsSender: " + messageCounter);
        if (queue.isEmpty() && needToStop) {
            log.info("stopping JmsSender...");
            this.interrupt();
        }
    }
}
