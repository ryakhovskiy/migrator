package org.kr.db.migrator.writer;

import org.kr.db.migrator.settings.QueryBase;
import org.kr.db.migrator.writer.settings.Settings;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: I005144
 * Date: 01.07.13
 * Time: 13:32
 * To change this template use File | Settings | File Templates.
 */
public class SqlWriter implements Runnable {

    private static final int TIMEOUT_MS = 1000;
    private final Logger log = Logger.getLogger(SqlWriter.class);
    private final Settings.Query query;
    private final BlockingQueue<List> queue;
    private final Connection connection;
    private final PreparedStatement statement;

    public SqlWriter(Settings.Query query, BlockingQueue<List> queue, String jdbcUrl) throws SQLException {
        this.query = query;
        this.queue = queue;
        this.connection = DriverManager.getConnection(jdbcUrl);
        this.connection.setAutoCommit(false);
        if (query.getType() == QueryBase.QueryType.CALL)
            statement = connection.prepareCall(query.getSql());
        else if (query.getType() == QueryBase.QueryType.INSERT)
            statement = connection.prepareStatement(query.getSql());
        else
            statement = null;
        log.debug("SqlWriter created; query: " + query.getSql());
    }

    @Override
    public void run() {
        try {
            doSqlWriterJob();
        } catch (InterruptedException e) {
            log.debug("SqlWriter has been interrupted");
        } catch (Exception e) {
            log.error(e);
        } finally {
            dispose();
        }
    }

    private void doSqlWriterJob() throws InterruptedException {
        int totalExecuted = 0;
        log.debug("starting SqlWriter operations...");
        while (true) {
            if (Thread.currentThread().isInterrupted())
                throw new InterruptedException();
            List<Map<Integer, Object>> arguments = null;
            try {
                arguments = getArguments(totalExecuted);
                for (Map<Integer, Object> argMap : arguments)
                    executeSqlQuery(argMap);
                int[] results = statement.executeBatch();
                if (log.isTraceEnabled())
                    log.trace("Batch executed; query: " + query.getSql());
                totalExecuted += arguments.size();
                if (totalExecuted >= query.getCommitSize()) {
                    connection.commit();
                    totalExecuted = 0;
                    if (log.isDebugEnabled())
                        log.debug("Commited: " + query.getSql());
                }
            } catch (SQLException e) {
                if (null != arguments) {
                    String error = String.format("%s:%nQuery: %s%nArguments: %s%n", e.getMessage(), query.getSql(), arguments);
                    log.error(error, e);
                } else
                    log.error(e);
            }
        }
    }

    /**
     *
     * @param arguments
     * @throws SQLException
     */
    private void executeSqlQuery(Map<Integer, Object> arguments) throws SQLException {
        for (Map.Entry<Integer, Object> arg : arguments.entrySet())
            statement.setObject(arg.getKey(), arg.getValue());
        statement.addBatch();
        if (log.isTraceEnabled())
            log.trace("Batch added: " + query.getSql() + "; args: " + arguments);
    }

    /**
     *
     * @param totalExecuted
     * @return
     * @throws InterruptedException
     * @throws SQLException
     */
    private List<Map<Integer, Object>> getArguments(int totalExecuted) throws InterruptedException, SQLException {
        List<Map<String, String>> argValuesList = pollArgumentValues(totalExecuted);
        List<Settings.Query.QueryArgument> argDefinitions = query.getArguments();
        List<Map<Integer, Object>> arguments = new ArrayList<Map<Integer, Object>>();
        for (Map<String, String> argValues : argValuesList) {
            Map<Integer, Object> argMap = new HashMap<Integer, Object>();
            for (Settings.Query.QueryArgument argDef : argDefinitions) {
                String valueSource = argDef.getValueSource();
                String strValue = argValues.get(valueSource);
                if (strValue == null)
                    log.warn("No argument value for " + valueSource);
                Object value = convert(argDef.getType(), strValue);
                if (value == null)
                    log.warn("Argument value cannot be converted: valueSource" + valueSource + "; strValue: " + strValue + "; type: " + argDef.getType());
                argMap.put(argDef.getIndex(), value);
            }
            arguments.add(argMap);
        }
        return arguments;
    }

    /**
     *
     * @param totalExecuted
     * @return
     * @throws InterruptedException
     * @throws SQLException
     */
    private List<Map<String, String>> pollArgumentValues(int totalExecuted) throws InterruptedException, SQLException {
        List<Map<String, String>> argValuesList;
        while (true) {
            argValuesList = queue.poll(TIMEOUT_MS, TimeUnit.MILLISECONDS);
            if (log.isTraceEnabled()) {
                if (argValuesList != null && argValuesList.size() > 0)
                    log.trace("Arguments received: " + argValuesList);
                else
                    log.trace("Awaiting for arguments...");
            }
            if (argValuesList != null && argValuesList.size() > 0)
                break;
            //in case when we were waiting for a long time and no arguments were received, but a few batches were executed, commit changes.
            else if (totalExecuted > 0)
                connection.commit();
        }
        return argValuesList;
    }

    private Object convert(Settings.Query.ArgumentType type, String value) {
        switch (type) {
            case STRING:
                return value;
            case INT:
                return Integer.valueOf(value.trim());
            case LONG:
                return Long.valueOf(value.trim());
            case DATE:
                return Date.valueOf(value.trim());
            case TIME:
                return Time.valueOf(value.trim());
            case TIMESTAMP:
                return Timestamp.valueOf(value.trim());
            case FLOAT:
                return Float.valueOf(value.trim());
            case DOUBLE:
                return Double.valueOf(value.trim());
            default:
                throw new UnsupportedOperationException("specified argument type is not supported yet");
        }
    }

    private void dispose() {
        if (null != connection)
            try {
                connection.close();
            } catch (SQLException e) {
                log.error("Error while closing SQL Connection", e);
            }
    }
}
