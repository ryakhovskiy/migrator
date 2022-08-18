package org.kr.db.migrator.rw.settings;

import org.apache.log4j.Logger;

import java.sql.*;
import java.util.Map;

/**
 * Created by I005144 on 12.08.13.
 */
public class QueryBuilder {

    private final Logger log = Logger.getLogger(QueryBuilder.class);

    public static QueryBuilder newInstance() {
        return new QueryBuilder();
    }

    private QueryBuilder() { }

    public String createSelectStatement(String url, String tableName, String openEscapeSymbol, String closeEscapeSymbol) throws SQLException {
        String newName = createTableName(tableName, openEscapeSymbol, closeEscapeSymbol);
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = DriverManager.getConnection(url);
            final String query = "select * from " + newName;
            log.debug("executing: " + query);
            statement = connection.prepareStatement(query);
            statement.executeQuery();
            ResultSetMetaData rsMetadata = statement.getMetaData();
            int columnsCount = rsMetadata.getColumnCount();
            String[] columns = new String[columnsCount];
            for (int i = 0; i < columnsCount; i++)
                columns[i] = rsMetadata.getColumnName(i + 1); //JDBC column indexes starts with 1 (not 0)
            return createSelectStatement(tableName, columns, openEscapeSymbol, closeEscapeSymbol);
        } finally {
            if (null != connection)
                connection.close();
            if (null != statement)
                statement.close();
        }
    }

    public String createSelectStatement(String tableName, String[] columns, String openEscapeSymbol, String closeEscapeSymbol) {
        String newName = createTableName(tableName, openEscapeSymbol, closeEscapeSymbol);
        StringBuilder queryBuilder = new StringBuilder("select ");
        for (String column : columns) {
            if (null != openEscapeSymbol && openEscapeSymbol.length() > 0)
                queryBuilder.append(openEscapeSymbol);
            queryBuilder.append(column);
            if (null != closeEscapeSymbol && closeEscapeSymbol.length() > 0)
                queryBuilder.append(closeEscapeSymbol);
            queryBuilder.append(',');
        }
        queryBuilder.deleteCharAt(queryBuilder.length() - 1);
        queryBuilder.append(" from ");
        queryBuilder.append(newName);
        log.debug("select query created: " + queryBuilder.toString());
        return queryBuilder.toString();
    }

    public String createInsertStatement(String url, String tableName, String openEscapeSymbol, String closeEscapeSymbol, Map<String, Integer> argMapping) throws SQLException {
        String newName = createTableName(tableName, openEscapeSymbol, closeEscapeSymbol);
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = DriverManager.getConnection(url);
            final String query = "select * from " + newName;
            log.debug("executing: " + query);
            statement = connection.prepareStatement(query);
            ResultSetMetaData rsMetadata = statement.getMetaData();
            int columnsCount = rsMetadata.getColumnCount();
            String[] columns = new String[columnsCount];
            for (int i = 0; i < columnsCount; i++)
                columns[i] = rsMetadata.getColumnName(i + 1); //JDBC column indexes starts with 1 (not 0)
            return createInsertStatement(tableName, columns, openEscapeSymbol, closeEscapeSymbol, argMapping);
        } finally {
            if (null != connection)
                connection.close();
            if (null != statement)
                statement.close();
        }
    }

    public String createInsertStatement(String tableName, String[] columns, String openEscapeSymbol, String closeEscapeSymbol, Map<String, Integer> argMapping) {
        String newName = createTableName(tableName, openEscapeSymbol, closeEscapeSymbol);
        StringBuilder queryBuilder = new StringBuilder("insert into ");
        queryBuilder.append(newName);
        queryBuilder.append(" (");
        int columnCounter = 1;
        for (String column : columns) {
            if (null != openEscapeSymbol && openEscapeSymbol.length() > 0)
                queryBuilder.append(openEscapeSymbol);
            queryBuilder.append(column);
            if (null != closeEscapeSymbol && closeEscapeSymbol.length() > 0)
                queryBuilder.append(closeEscapeSymbol);
            queryBuilder.append(',');
            argMapping.put(column, columnCounter++);
        }
        //delete last comma
        queryBuilder.deleteCharAt(queryBuilder.length() - 1);
        queryBuilder.append(") values (");
        for (int i = 0; i < columns.length; i++)
            queryBuilder.append("?,");
        //delete last comma
        queryBuilder.deleteCharAt(queryBuilder.length() - 1);
        queryBuilder.append(')');
        log.debug("insert query created: " + queryBuilder.toString());
        return queryBuilder.toString();
    }

    private String createTableName(String tableName, String openEscapeSymbol, String closeEscapeSymbol) {
        String[] tokens = tableName.split("\\.");
        StringBuilder newName = new StringBuilder();
        for (int i = 0; i < tokens.length; i++) {
            newName.append(openEscapeSymbol);
            newName.append(tokens[i]);
            newName.append(closeEscapeSymbol);
            newName.append(".");
        }
        return newName.deleteCharAt(newName.length() - 1).toString();
    }

}
