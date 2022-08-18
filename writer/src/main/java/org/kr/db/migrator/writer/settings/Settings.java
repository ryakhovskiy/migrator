package org.kr.db.migrator.writer.settings;

import org.kr.db.migrator.settings.ConnectionInfo;
import org.kr.db.migrator.settings.QueryBase;
import org.kr.db.migrator.settings.SettingsBase;

import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: I005144
 * Date: 01.07.13
 * Time: 10:47
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

        private final int commitSize;
        private final int jmsReaders;
        private final int sqlWriters;
        private final List<QueryArgument> queryArguments = new ArrayList<QueryArgument>();

        public Query(QueryBase.QueryType type, int commitSize, String queueTopic, String sql, int jmsReaders, int sqlWriters, int queueCapacity) {
            super(sql, type, queueTopic, queueCapacity);
            this.commitSize = commitSize;
            this.jmsReaders = jmsReaders;
            this.sqlWriters = sqlWriters;
        }

        public void addArgument(int argIndex, ArgumentType type, String valueSource) {
            queryArguments.add(new QueryArgument(argIndex, type, valueSource));
        }

        public void setArguments(List<QueryArgument> queryArguments) {
            this.queryArguments.clear();
            this.queryArguments.addAll(queryArguments);
        }

        public List<QueryArgument> getArguments() {
            return new ArrayList<QueryArgument>(queryArguments);
        }

        public int getSqlWriters() {
            return sqlWriters;
        }

        public int getJmsReaders() {
            return jmsReaders;
        }

        public QueryType getType() {
            return super.getType();
        }

        public int getCommitSize() {
            return commitSize;
        }

        public String getQueueTopic() {
            return super.getQueueTopic();
        }

        public String getSql() {
            return super.getSql();
        }

        public String toString() {
            return String.format("JMS Readers-SQL Writers: %d-%d; Type: %s; Commit size: %d; QueueTopic: %s; SQL Query: %s", jmsReaders, sqlWriters, super.getType(), commitSize, super.getQueueTopic(), super.getSql());
        }

        public enum ArgumentType {
            STRING,
            INT,
            LONG,
            DATE,
            TIME,
            TIMESTAMP,
            FLOAT,
            DOUBLE
        }

        public static class QueryArgument {
            private int index;
            private ArgumentType type;
            private String valueSource;

            public QueryArgument(int index, ArgumentType type, String valueSource) {
                this.index = index;
                this.type = type;
                this.valueSource = valueSource;
            }

            public int getIndex() {
                return index;
            }

            public ArgumentType getType() {
                return type;
            }

            public String getValueSource() {
                return valueSource;
            }
        }
    }
}
