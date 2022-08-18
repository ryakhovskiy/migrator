package org.kr.db.migrator.reader;

import org.kr.db.migrator.reader.settings.Settings;
import org.kr.db.migrator.settings.QueryBase;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

/**
 * Created with IntelliJ IDEA.
 * User: I005144
 * Date: 28.06.13
 * Time: 17:44
 * To change this template use File | Settings | File Templates.
 */
public class SqlReader implements Runnable {

    private final Logger log = Logger.getLogger(SqlReader.class);
    private final String jdbcUrl;
    private final Settings.Query query;
    private final BlockingQueue<List> queue;

    public SqlReader(String jdbcUrl, Settings.Query query, BlockingQueue<List> queue) {
        this.jdbcUrl = jdbcUrl;
        this.query = query;
        this.queue = queue;
        log.info("SqlReader created;");
    }

    @Override
    public void run() {
        try {
            executeQuery();
        } catch (SQLException e) {
            log.error("SQLException thrown...", e);
        } catch (InterruptedException e) {
            log.error("SqlReader has been interrupted");
        }
    }

    private void executeQuery() throws SQLException, InterruptedException{
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        try {
            connection = DriverManager.getConnection(jdbcUrl);
            log.info("SqlReader established connection with database");
            connection.setAutoCommit(false);
            if (query.getType() == QueryBase.QueryType.CALL)
                statement = connection.prepareCall(query.getSql());
            else if (query.getType() == QueryBase.QueryType.SELECT)
                statement = connection.prepareStatement(query.getSql());
            else
                throw new UnsupportedOperationException("SqlReader supports only SELECT and CALL query types.");
            log.info("SqlReader prepared sql statement");
            statement.setFetchSize(query.getFetchSize());
            resultSet = statement.executeQuery();
            resultSet.setFetchSize(query.getFetchSize());
            List<String> columnNames = getColumnNames(resultSet);
            log.info("SqlReader read query metadata");
            List<Map> results = new ArrayList<Map>();
            log.info("Starting reading batches and putting messages to internal queue...");
            while (resultSet.next()) {
                if (Thread.currentThread().isInterrupted())
                    throw new InterruptedException();
                handleNextResult(resultSet, columnNames, results);
            }
            //send if list is not empty
            if (results.size() > 0)
                sendResults(results);
            log.info("Data has been retrieved completely");
        } finally {
            if (null != resultSet)
                resultSet.close();
            if (null != statement)
                statement.close();
            if (null != connection)
                connection.close();
        }
    }

    private void handleNextResult(ResultSet resultSet, List<String> columnNames, List<Map> results) throws SQLException, InterruptedException {
        Map<String, String> result = new HashMap<String, String>();
        for (String column : columnNames)
            result.put(column, resultSet.getString(column).intern());
        results.add(result);
        if (results.size() == query.getBatchSize())
            sendResults(results);
    }

    private List<String> getColumnNames(ResultSet resultSet) throws SQLException {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        int columnsCount = resultSetMetaData.getColumnCount();
        List<String> columnNames = new ArrayList<String>(columnsCount);
        for (int i = 1; i <= columnsCount; i++)
            columnNames.add(resultSetMetaData.getColumnName(i));
        return columnNames;
    }

    private void sendResults(List results) throws InterruptedException {
        List copy = new ArrayList(results);
        results.clear();
        if (log.isTraceEnabled())
            log.trace("putting message to internal queue: " + copy);
        queue.put(copy);
    }
}
