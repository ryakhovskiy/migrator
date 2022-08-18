package org.kr.db.migrator.rw.settings;

/**
 * Created with IntelliJ IDEA.
 * User: I005144
 * Date: 04.07.13
 * Time: 14:22
 * To change this template use File | Settings | File Templates.
 */
public class ConnectionInfo {

    private final DatabaseConnectionInfo sourceDbConnectionInfo;
    private final DatabaseConnectionInfo destinationDbConnectionInfo;

    public ConnectionInfo(DatabaseConnectionInfo sourceDbConnectionInfo, DatabaseConnectionInfo destinationDbConnectionInfo) {
        this.sourceDbConnectionInfo = sourceDbConnectionInfo;
        this.destinationDbConnectionInfo = destinationDbConnectionInfo;
    }

    public DatabaseConnectionInfo getSourceDbConnectionInfo() {
        return sourceDbConnectionInfo;
    }

    public DatabaseConnectionInfo getDestinationDbConnectionInfo() {
        return destinationDbConnectionInfo;
    }

    public static class DatabaseConnectionInfo {

        private final String driver;
        private final String url;

        public DatabaseConnectionInfo(String driver, String url) {
            this.driver = driver;
            this.url = url;
        }

        public String getDriver() {
            return driver;
        }

        public String getUrl() {
            return url;
        }

        public String toString() {
            return String.format("Driver: %s; URL: %s", driver, url);
        }
    }
}
