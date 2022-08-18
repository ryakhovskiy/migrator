package org.kr.db.migrator.rw;

import org.kr.db.migrator.rw.settings.QueryBase;
import org.kr.db.migrator.rw.settings.QueryRead;
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
 * Date: 04.07.13
 * Time: 14:20
 * To change this template use File | Settings | File Templates.
 */
public class SqlReader implements Runnable {

    private final Logger log = Logger.getLogger(SqlReader.class);
    private final String jdbcUrl;
    private final BlockingQueue<Map> queue;
    private final QueryRead query;

    public SqlReader(String jdbcUrl, QueryRead query, BlockingQueue<Map> queue) {
        this.jdbcUrl = jdbcUrl;
        this.queue = queue;
        this.query = query;
        log.info("SqlReader created, query: " + query);
    }


    @Override
    public void run() {
        try {
            performReaderWork();
        } catch (SQLException e) {
            log.error(e);
        } catch (InterruptedException e) {
            log.debug("SqlReader has been interrupted");
        }
    }

    private void performReaderWork() throws SQLException, InterruptedException {
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        try {
            connection = prepareConnection();
            statement = prepareStatement(connection);
            resultSet = statement.executeQuery();
            resultSet.setFetchSize(query.getFetchSize());
            List<String> columnNames = getColumnNames(resultSet);
            log.info("SqlReader read query metadata");
            handleResults(resultSet, columnNames);
        } finally {
            if (null != resultSet)
                resultSet.close();
            if (null != statement)
                statement.close();
            if (null != connection)
                connection.close();
        }

    }

    private Connection prepareConnection() throws SQLException {
        Connection connection = null;
        connection = DriverManager.getConnection(jdbcUrl);
        connection.setAutoCommit(false);
        log.info("SqlReader established connection with database");
        return connection;
    }

    private PreparedStatement prepareStatement(Connection connection) throws SQLException {
        PreparedStatement statement = null;
        if (query.getQueryType() == QueryBase.QueryType.SELECT)
            statement = connection.prepareStatement(query.getSqlStatement());
        else if (query.getQueryType() == QueryBase.QueryType.CALL)
            statement = connection.prepareCall(query.getSqlStatement());
        else
            throw new UnsupportedOperationException("SqlReader supports only SELECT or CALL type. " + query.getQueryType() + " is not supported by SqlReader");
        statement.setFetchSize(query.getFetchSize());
        log.info("SqlReader prepared sql statement");
        return statement;
    }

    private void handleResults(ResultSet resultSet, List<String> columnNames) throws SQLException, InterruptedException {
        log.info("Starting reading batches and putting messages to internal queue...");
        int resultsCounter = 0;
        while (resultSet.next()) {
            Map<String, Object> result = new HashMap<String, Object>();
            for (String column : columnNames)
                result.put(column, resultSet.getObject(column));
            if (++resultsCounter % 1000 == 0)
                log.info("Total records read: " + resultsCounter);
            queue.put(result);
        }
        log.info("Data has been read completely. Total records count: " + resultsCounter);
    }

    private List<String> getColumnNames(ResultSet resultSet) throws SQLException {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        int columnsCount = resultSetMetaData.getColumnCount();
        List<String> columnNames = new ArrayList<String>(columnsCount);
        for (int i = 1; i <= columnsCount; i++)
            columnNames.add(resultSetMetaData.getColumnName(i));
        return columnNames;
    }

}
