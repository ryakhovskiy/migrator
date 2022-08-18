package org.kr.db.migrator.reader;

import org.kr.db.migrator.exceptions.ReadSettingsException;
import org.kr.db.migrator.reader.settings.Settings;
import org.kr.db.migrator.reader.settings.SettingsReader;
import org.kr.db.migrator.settings.QueryBase;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.sql.SQLException;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: I005144
 * Date: 28.06.13
 * Time: 15:43
 * To change this template use File | Settings | File Templates.
 */
public class ReaderMain {

    static {
        PropertyConfigurator.configure("log4j.properties");
    }

    private static final Logger log = Logger.getLogger(ReaderMain.class);

    public static void main(String... args) {
        try {
            Settings settings = readApplicationSettings();
            System.out.println(settings);
            String jdbcDriver = settings.getConnectionInfo().getDatabaseConnectionInfo().getDriver();
            registerJdbcDriver(jdbcDriver);
            String jdbcUrl = settings.getConnectionInfo().getDatabaseConnectionInfo().getUrl();
            String jmsBrokerUrl = settings.getConnectionInfo().getJmsConnectionInfo().getBrokerUrl();
            startReaderWorkers(settings.getQueryList(), jdbcUrl, jmsBrokerUrl);
        } catch (Exception e) {
            log.error(e);
        }
    }

    private static void startReaderWorkers(List<? extends QueryBase> queryList, String jdbcUrl, String jmsBrokerUrl) throws SQLException {
        for (QueryBase queryBase : queryList) {
            Settings.Query query = (Settings.Query)queryBase;
            new ReaderWorker(query, jdbcUrl, jmsBrokerUrl).startWorker();
        }
    }

    private static Settings readApplicationSettings() throws ReadSettingsException {
        log.debug("reading Application settings...");
        Settings settings = SettingsReader.readAllSettings();
        return settings;
    }

    private static void registerJdbcDriver(String jdbcDriver) throws ClassNotFoundException {
        log.debug("registering JDBC driver: " + jdbcDriver);
        Class.forName(jdbcDriver);
    }

}
