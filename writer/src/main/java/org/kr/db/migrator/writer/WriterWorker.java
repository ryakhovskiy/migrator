package org.kr.db.migrator.writer;

import org.kr.db.migrator.NamedThreadFactory;
import org.kr.db.migrator.writer.settings.Settings;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created with IntelliJ IDEA.
 * User: I005144
 * Date: 01.07.13
 * Time: 15:21
 * To change this template use File | Settings | File Templates.
 */
public class WriterWorker {

    private final BlockingQueue<List> queue;
    private final List<JmsReader> jmsReaders = new ArrayList<JmsReader>();
    private final List<SqlWriter> sqlWriters = new ArrayList<SqlWriter>();
    private final ExecutorService jmsReadersExecutors;
    private final ExecutorService sqlWritersExecutors;

    public WriterWorker(Settings.Query query, String jdbcUrl, String jmsBrokerUrl) throws SQLException {

        if (query.getQueueCapacity() > 0)
            queue = new LinkedBlockingQueue<List>(query.getQueueCapacity());
        else
            queue = new LinkedBlockingQueue<List>();

        for (int i = 0; i < query.getJmsReaders(); i++)
            jmsReaders.add(new JmsReader(jmsBrokerUrl, query.getQueueTopic(), queue));
        for (int i = 0; i < query.getSqlWriters(); i++)
            sqlWriters.add(new SqlWriter(query, queue, jdbcUrl));

        jmsReadersExecutors = Executors.newFixedThreadPool(query.getJmsReaders(), NamedThreadFactory.getNamedThreadFactory("JmsReader"));
        sqlWritersExecutors = Executors.newFixedThreadPool(query.getSqlWriters(), NamedThreadFactory.getNamedThreadFactory("SqlWriter"));
    }

    public void startWorker() {
        for (int i = 0; i < jmsReaders.size(); i++)
            jmsReadersExecutors.submit(jmsReaders.get(i));
        for (int i = 0; i < sqlWriters.size(); i++)
            sqlWritersExecutors.submit(sqlWriters.get(i));
    }
}
