package org.kr.db.migrator.rw;

import org.kr.db.migrator.NamedThreadFactory;
import org.kr.db.migrator.rw.settings.Action;
import org.kr.db.migrator.rw.settings.ConnectionInfo;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Created with IntelliJ IDEA.
 * User: I005144
 * Date: 04.07.13
 * Time: 14:21
 * To change this template use File | Settings | File Templates.
 */
public class RWWorker implements Runnable {

    private final Logger log = Logger.getLogger(RWWorker.class);
    private final Action action;
    private final ExecutorService executorService;
    private final List<SqlWriter> sqlWriters = new ArrayList<SqlWriter>();
    private final Thread sqlReaderThread;

    public RWWorker(ConnectionInfo connectionInfo, Action action) throws SQLException {
        this.action = action;
        String readerName = action.getQueryRead().getThreadPrefix();
        String writerName = action.getQueryWrite().getThreadPrefix();
        BlockingQueue<Map> queue = createQueue(action.getQueueCapacity());
        SqlReader sqlReader = new SqlReader(connectionInfo.getSourceDbConnectionInfo().getUrl(), action.getQueryRead(), queue);
        sqlReaderThread = new Thread(sqlReader, readerName);
        executorService = Executors.newFixedThreadPool(action.getWritersCount(), NamedThreadFactory.getNamedThreadFactory(writerName));
        createSqlWriters(connectionInfo.getDestinationDbConnectionInfo().getUrl(), queue);
    }

    public void startWorkers() throws InterruptedException {
        for (SqlWriter sqlWriter : sqlWriters)
            executorService.submit(sqlWriter);
        sqlReaderThread.start();
        sqlReaderThread.join();
        for (SqlWriter sqlWriter : sqlWriters)
            sqlWriter.suggestToStop();
        executorService.shutdown();
    }

    private void createSqlWriters(String jdbcUrl, BlockingQueue<Map> queue) throws SQLException {
        for (int i = 0; i < action.getWritersCount(); i++) {
            SqlWriter sqlWriter = new SqlWriter(jdbcUrl, action.getQueryWrite(), queue);
            sqlWriters.add(sqlWriter);
        }
    }

    private BlockingQueue<Map> createQueue(int capacity) {
        if (capacity > 0)
            return new LinkedBlockingQueue<Map>(capacity);
        else
            return new LinkedBlockingQueue<Map>();
    }

    @Override
    public void run() {
        try {
            startWorkers();
        } catch (InterruptedException e) {
            log.debug("RWWorker has been interrupted");
        }
    }
}
