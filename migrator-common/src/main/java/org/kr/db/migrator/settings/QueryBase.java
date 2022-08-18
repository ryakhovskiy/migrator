package org.kr.db.migrator.settings;

/**
 * Created with IntelliJ IDEA.
 * User: I005144
 * Date: 01.07.13
 * Time: 11:34
 * To change this template use File | Settings | File Templates.
 */
public abstract class QueryBase {

    private final String sql;
    private final QueryType type;
    private final String queueTopic;
    private final int queueCapacity;

    public QueryBase(String sql, QueryType type, String queueTopic, int queueCapacity) {
        this.sql = sql;
        this.type = type;
        this.queueTopic = queueTopic;
        this.queueCapacity = queueCapacity;
    }

    public String getSql() {
        return sql;
    }

    public QueryType getType() {
        return type;
    }

    public String getQueueTopic() {
        return queueTopic;
    }

    public int getQueueCapacity() {
        return queueCapacity;
    }

    public enum QueryType {
        SELECT, INSERT, CALL
    }
}
