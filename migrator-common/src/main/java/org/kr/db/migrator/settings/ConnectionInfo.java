package org.kr.db.migrator.settings;

/**
 * Created with IntelliJ IDEA.
 * User: I005144
 * Date: 01.07.13
 * Time: 10:53
 * To change this template use File | Settings | File Templates.
 */
public class ConnectionInfo {

    private DatabaseConnectionInfo databaseConnectionInfo;
    private JmsConnectionInfo jmsConnectionInfo;

    public ConnectionInfo(DatabaseConnectionInfo databaseConnectionInfo, JmsConnectionInfo jmsConnectionInfo) {
        this.databaseConnectionInfo = databaseConnectionInfo;
        this.jmsConnectionInfo = jmsConnectionInfo;
    }

    public DatabaseConnectionInfo getDatabaseConnectionInfo() {
        return databaseConnectionInfo;
    }

    public JmsConnectionInfo getJmsConnectionInfo() {
        return jmsConnectionInfo;
    }

    public String toString() {
        return String.format("DatabaseConnectionInfo: %s%nJmsConnectionInfo: %s", databaseConnectionInfo, jmsConnectionInfo);
    }

    public static class DatabaseConnectionInfo {

        private String driver;
        private String url;

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

    public static class JmsConnectionInfo {

        private String brokerUrl;

        public JmsConnectionInfo(String brokerUrl) {
            this.brokerUrl = brokerUrl;
        }

        public String getBrokerUrl() {
            return brokerUrl;
        }

        public String toString() {
            return String.format("BrokerURL: %s", brokerUrl);
        }
    }
}
