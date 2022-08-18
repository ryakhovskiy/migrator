package org.kr.db.migrator.rw.settings;

import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: I005144
 * Date: 04.07.13
 * Time: 14:48
 * To change this template use File | Settings | File Templates.
 */
public class QueryWrite extends QueryBase {

    private final int commitSize;
    private final int batchSize;

    /**
     * here name of source column should correspond to index in destination column
     */
    private final Map<String, Integer> argumentMapping;

    public QueryWrite(String sqlStatement, QueryType queryType, int batchSize, String threadPrefix, int commitSize, Map<String, Integer> argumentMapping) {
        super(sqlStatement, queryType, threadPrefix);
        this.batchSize = batchSize;
        this.commitSize = commitSize;
        this.argumentMapping = argumentMapping;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getCommitSize() {
        return commitSize;
    }

    public Map<String, Integer> getArgumentMapping() {
        return argumentMapping;
    }
}
