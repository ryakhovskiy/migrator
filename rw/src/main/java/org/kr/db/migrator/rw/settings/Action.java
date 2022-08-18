package org.kr.db.migrator.rw.settings;

/**
 * Created with IntelliJ IDEA.
 * User: I005144
 * Date: 04.07.13
 * Time: 14:56
 * To change this template use File | Settings | File Templates.
 */
public class Action {

    private final QueryRead queryRead;
    private final QueryWrite queryWrite;
    private final int writersCount;
    private final int queueCapacity;

    public Action(QueryRead queryRead, QueryWrite queryWrite, int writersCount, int queueCapacity) {
        this.queryRead = queryRead;
        this.queryWrite = queryWrite;
        this.writersCount = writersCount;
        this.queueCapacity = queueCapacity;
    }

    public QueryRead getQueryRead() {
        return queryRead;
    }

    public QueryWrite getQueryWrite() {
        return queryWrite;
    }

    public int getWritersCount() {
        return writersCount;
    }

    public int getQueueCapacity() {
        return queueCapacity;
    }
}
