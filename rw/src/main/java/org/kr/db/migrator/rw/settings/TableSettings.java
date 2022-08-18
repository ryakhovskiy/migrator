package org.kr.db.migrator.rw.settings;

/**
 * Created by I005144 on 13.08.13.
 */
public class TableSettings {

    private int fetchSize;
    private int batchSize;
    private int commitSize;
    private int sqlWriters;
    private int queueCapacity;
    private String sourceOpenEscapeSymbol;
    private String sourceCloseEscapeSymbol;
    private String destOpenEscapeSymbol;
    private String destCloseEscapeSymbol;

    public String getDestOpenEscapeSymbol() {
        return destOpenEscapeSymbol;
    }

    public void setDestOpenEscapeSymbol(String destOpenEscapeSymbol) {
        this.destOpenEscapeSymbol = destOpenEscapeSymbol;
    }

    public String getDestCloseEscapeSymbol() {
        return destCloseEscapeSymbol;
    }

    public void setDestCloseEscapeSymbol(String destCloseEscapeSymbol) {
        this.destCloseEscapeSymbol = destCloseEscapeSymbol;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getCommitSize() {
        return commitSize;
    }

    public void setCommitSize(int commitSize) {
        this.commitSize = commitSize;
    }

    public int getSqlWriters() {
        return sqlWriters;
    }

    public void setSqlWriters(int sqlWriters) {
        this.sqlWriters = sqlWriters;
    }

    public int getQueueCapacity() {
        return queueCapacity;
    }

    public void setQueueCapacity(int queueCapacity) {
        this.queueCapacity = queueCapacity;
    }

    public String getSourceOpenEscapeSymbol() {
        return sourceOpenEscapeSymbol;
    }

    public void setSourceOpenEscapeSymbol(String sourceOpenEscapeSymbol) {
        this.sourceOpenEscapeSymbol = sourceOpenEscapeSymbol;
    }

    public String getSourceCloseEscapeSymbol() {
        return sourceCloseEscapeSymbol;
    }

    public void setSourceCloseEscapeSymbol(String sourceCloseEscapeSymbol) {
        this.sourceCloseEscapeSymbol = sourceCloseEscapeSymbol;
    }

    public TableSettings clone() {
        TableSettings tableSettings = new TableSettings();
        tableSettings.setDestCloseEscapeSymbol(destCloseEscapeSymbol);
        tableSettings.setDestOpenEscapeSymbol(destOpenEscapeSymbol);
        tableSettings.setSourceCloseEscapeSymbol(sourceCloseEscapeSymbol);
        tableSettings.setSourceOpenEscapeSymbol(sourceOpenEscapeSymbol);
        tableSettings.setBatchSize(batchSize);
        tableSettings.setCommitSize(commitSize);
        tableSettings.setFetchSize(fetchSize);
        tableSettings.setQueueCapacity(queueCapacity);
        tableSettings.setSqlWriters(sqlWriters);
        return tableSettings;
    }

}
