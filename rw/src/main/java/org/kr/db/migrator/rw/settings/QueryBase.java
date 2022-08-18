package org.kr.db.migrator.rw.settings;

/**
 * Created with IntelliJ IDEA.
 * User: I005144
 * Date: 04.07.13
 * Time: 14:25
 * To change this template use File | Settings | File Templates.
 */
public class QueryBase {

    private final String sqlStatement;
    private final QueryType queryType;
    private final String threadPrefix;

    public QueryBase(String sqlStatement, QueryType queryType, String threadPrefix) {
        this.sqlStatement = sqlStatement;
        this.queryType = queryType;
        this.threadPrefix = threadPrefix;
    }

    public String getSqlStatement() {
        return sqlStatement;
    }

    public QueryType getQueryType() {
        return queryType;
    }

    public String getThreadPrefix() {
        return threadPrefix;
    }

    public enum QueryType {
        SELECT, INSERT, CALL
    }
}
