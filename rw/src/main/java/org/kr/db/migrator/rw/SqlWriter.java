package org.kr.db.migrator.rw;

import org.kr.db.migrator.rw.settings.QueryBase;
import org.kr.db.migrator.rw.settings.QueryWrite;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: I005144
 * Date: 04.07.13
 * Time: 14:21
 * To change this template use File | Settings | File Templates.
 */
public class SqlWriter implements Runnable {

    private static final int TIMEOUT_MS = 2 * 1000;
    private final Logger log = Logger.getLogger(SqlWriter.class);
    private final BlockingQueue<Map> queue;
    private final QueryWrite query;
    private final Connection connection;
    private final PreparedStatement statement;
    private volatile boolean needToStop = false;

    public SqlWriter(String jdbcUrl, QueryWrite query, BlockingQueue<Map> queue) throws SQLException {
        this.queue = queue;
        this.query = query;
        connection = prepareConnection(jdbcUrl);
        statement = prepareStatement(connection);
    }

    @Override
    public void run() {
        try {
            performSqlWriterWork();
        } catch (SQLException e) {
            log.error(e);
        } catch (InterruptedException e) {
            log.debug("SqlWriter has been interrupted");
        }
    }

    private void performSqlWriterWork() throws SQLException, InterruptedException {
        try {
            int toBeCommitted = 0;
            while (true) {
                Map<String, Object> arguments = pollArguments(toBeCommitted);
                performWriteAction(arguments);
                toBeCommitted++;
                if (toBeCommitted == query.getBatchSize()) {
                    int[] resCount = statement.executeBatch();
                    if (log.isTraceEnabled())
                        log.trace("Batch executed: " + query.getSqlStatement());
                }
                if (toBeCommitted == query.getCommitSize()) {
                    connection.commit();
                    toBeCommitted = 0;
                    log.debug("Committed: " + query.getSqlStatement());
                }
            }
        } finally {
            if (null != statement)
                statement.close();
            if (null != connection)
                connection.close();
        }
    }

    public synchronized void suggestToStop() {
        this.needToStop = true;
    }

    private void performWriteAction(Map<String, Object> arguments) throws SQLException {
        //set arguments
        for (Map.Entry<String, Integer> argMapEntry : query.getArgumentMapping().entrySet()) {
            Object value = arguments.get(argMapEntry.getKey());
            if (null == value)
                log.debug("No value found for " + argMapEntry.getKey() + " or value is null. Search in: " + arguments.keySet());
            statement.setObject(argMapEntry.getValue(), value);
        }
        statement.addBatch();
    }

    private Connection prepareConnection(String jdbcUrl) throws SQLException {
        Connection connection = DriverManager.getConnection(jdbcUrl);
        connection.setAutoCommit(false);
        log.debug("SqlWriter established connection with database");
        return connection;
    }

    private PreparedStatement prepareStatement(Connection connection) throws SQLException {
        PreparedStatement statement = null;
        if (query.getQueryType() == QueryBase.QueryType.INSERT)
            statement = connection.prepareStatement(query.getSqlStatement());
        else if (query.getQueryType() == QueryBase.QueryType.CALL)
            statement = connection.prepareCall(query.getSqlStatement());
        else
            throw new UnsupportedOperationException("SqlWriter supports only INSERT or CALL type. " + query.getQueryType() + " is not supported by SqlWriter");
        log.debug("SqlWriter prepared sql statement: " + query.getSqlStatement());
        return statement;
    }

    private Map<String, Object> pollArguments(int toBeCommitted) throws InterruptedException, SQLException {
        Map<String, Object> arguments = null;
        int attempt = 0;
        while (true) {
            arguments = queue.poll(TIMEOUT_MS, TimeUnit.MILLISECONDS);
            attempt++;
            if (arguments != null && arguments.size() > 0)
                return arguments;
            //in case when we were waiting for a long time and no arguments were received, but a few batches were executed, commit changes.
            else {
                if (toBeCommitted > 0) {
                    commitEnqueuedData();
                    log.trace("Attempt to poll arguments from queue: " + attempt);
                    toBeCommitted = 0;
                } else {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private void commitEnqueuedData() throws SQLException {
        statement.executeBatch();
        connection.commit();
        log.debug("Committed, long wait: " + query.getSqlStatement());
    }
}
