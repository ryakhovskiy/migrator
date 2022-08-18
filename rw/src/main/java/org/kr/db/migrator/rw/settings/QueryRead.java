package org.kr.db.migrator.rw.settings;

/**
 * Created with IntelliJ IDEA.
 * User: I005144
 * Date: 04.07.13
 * Time: 14:43
 * To change this template use File | Settings | File Templates.
 */
public class QueryRead extends QueryBase {

    private final int fetchSize;

    public QueryRead(String sqlStatement, QueryType queryType, String threadPrefix, int fetchSize) {
        super(sqlStatement, queryType, threadPrefix);
        this.fetchSize = fetchSize;
    }

    public int getFetchSize() {
        return fetchSize;
    }

}
