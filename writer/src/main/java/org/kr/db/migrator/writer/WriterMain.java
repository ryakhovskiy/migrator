package org.kr.db.migrator.writer;

import org.kr.db.migrator.settings.QueryBase;
import org.kr.db.migrator.writer.settings.Settings;
import org.kr.db.migrator.writer.settings.SettingsReader;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * Created with IntelliJ IDEA.
 * User: I005144
 * Date: 29.06.13
 * Time: 17:30
 * To change this template use File | Settings | File Templates.
 */
public class WriterMain {

    static {
        PropertyConfigurator.configure("log4j.properties");
    }

    private static final Logger log = Logger.getLogger(WriterMain.class);

    public static void main(String... args) {
        try {
            Settings settings = SettingsReader.readAllSettings();
            registerJdbcDriver(settings.getConnectionInfo().getDatabaseConnectionInfo().getDriver());
            String jdbcUrl = settings.getConnectionInfo().getDatabaseConnectionInfo().getUrl();
            String jmsBrokerUrl = settings.getConnectionInfo().getJmsConnectionInfo().getBrokerUrl();
            for (QueryBase queryBase : settings.getQueryList()) {
                Settings.Query query = (Settings.Query)queryBase;
                WriterWorker writerWorker = new WriterWorker(query, jdbcUrl, jmsBrokerUrl);
                writerWorker.startWorker();
            }
        } catch (Exception e) {
            log.error(e);
        }
    }

    private static void registerJdbcDriver(String jdbcDriver) throws ClassNotFoundException {
        log.debug("registering JDBC driver: " + jdbcDriver);
        Class.forName(jdbcDriver);
    }

}
