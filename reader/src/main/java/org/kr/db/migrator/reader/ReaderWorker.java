package org.kr.db.migrator.reader;

import org.kr.db.migrator.reader.settings.Settings;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created with IntelliJ IDEA.
 * User: I005144
 * Date: 02.07.13
 * Time: 20:01
 * To change this template use File | Settings | File Templates.
 */
public class ReaderWorker {

    private final Logger log = Logger.getLogger(ReaderWorker.class);
    private final BlockingQueue<List> queue;
    private final JmsSender jmsSender;
    private final SqlReader sqlReader;
    private final ThreadFactory threadFactory = new SqlReaderThreadFactory("SqlReader");

    public ReaderWorker(Settings.Query query, String jdbcUrl, String jmsBrokerUrl) throws SQLException {

        if (query.getQueueCapacity() > 0)
            queue = new LinkedBlockingQueue<List>(query.getQueueCapacity());
        else
            queue = new LinkedBlockingQueue<List>();

        jmsSender = new JmsSender(jmsBrokerUrl, query.getQueueTopic(), queue, query.getThreadName());
        sqlReader = new SqlReader(jdbcUrl, query, queue);
    }

    public void startWorker() {
        jmsSender.start();
        Thread sqlReaderThread = threadFactory.newThread(sqlReader);
        sqlReaderThread.start();
        try {
            //wait once data will be read, then suggest to stop for corresponding JmsSender.
            sqlReaderThread.join();
            jmsSender.suggestToStop();
        } catch (InterruptedException e) {
            log.debug("ReaderWorker has been interrupted");
        }
    }

    private static class SqlReaderThreadFactory implements ThreadFactory {

        private String threadNameTemplate;
        private int counter;

        public SqlReaderThreadFactory(String threadNameTemplate) {
            this.threadNameTemplate = threadNameTemplate;
        }

        @Override
        public Thread newThread(Runnable r) {
            counter++;
            return new Thread(r, String.format(threadNameTemplate, "-", counter));
        }
    }

}
