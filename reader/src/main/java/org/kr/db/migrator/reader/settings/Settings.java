package org.kr.db.migrator.reader.settings;

import org.kr.db.migrator.settings.ConnectionInfo;
import org.kr.db.migrator.settings.QueryBase;
import org.kr.db.migrator.settings.SettingsBase;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: I005144
 * Date: 28.06.13
 * Time: 15:59
 * To change this template use File | Settings | File Templates.
 */
public class Settings extends SettingsBase {

    public Settings(ConnectionInfo connectionInfo, List<Settings.Query> queryList) {
        super(connectionInfo, queryList);
    }

    public String toString() {
        return String.format("ConnectionInfo: %s%nQueryList: %s", super.getConnectionInfo(), super.getQueryList());
    }

    public static class Query extends QueryBase {

        private final int fetchSize;
        private final int batchSize;
        private final String threadName;

        public Query(QueryType type, int fetchSize, int batchSize, String queueTopic, String sql, String threadName, int queueCapacity) {
            super(sql, type, queueTopic, queueCapacity);
            this.fetchSize = fetchSize;
            this.batchSize = batchSize;
            this.threadName = threadName;
        }

        public String getThreadName() {
            return threadName;
        }

        public QueryType getType() {
            return super.getType();
        }

        public int getBatchSize() {
            return batchSize;
        }

        public String getQueueTopic() {
            return super.getQueueTopic();
        }

        public String getSql() {
            return super.getSql();
        }

        public int getFetchSize() {
            return fetchSize;
        }



        public String toString() {
            return String.format("Thread: %s; Type: %s; BatchSize: %d; QueueTopic: %s; SQL Query: %s", threadName, super.getType(), batchSize, super.getQueueTopic(), super.getSql());
        }
    }
}
