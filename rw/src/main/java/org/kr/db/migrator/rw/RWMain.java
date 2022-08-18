package org.kr.db.migrator.rw;

import org.kr.db.migrator.rw.settings.*;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.sql.SQLException;

/**
 * Created with IntelliJ IDEA.
 * User: I005144
 * Date: 04.07.13
 * Time: 14:19
 * To change this template use File | Settings | File Templates.
 */
public class RWMain {

    static {
        PropertyConfigurator.configure("log4j.properties");
    }

    private static final Logger log = Logger.getLogger(RWMain.class);

    public static void main(String... args) throws SQLException {

        try {
            Settings settings = null;
            if (args.length == 0)
                settings = SettingsReader.readSettigns();
            else
                settings = SettingsReader.readSettigns(args[0]);
            for (Action action : settings.getActions()) {
                RWWorker rwWorker = new RWWorker(settings.getConnectionInfo(), action);
                new Thread(rwWorker, "rwWorker").start();
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

}
