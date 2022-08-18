package org.kr.db.migrator.rw.settings;

import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: I005144
 * Date: 04.07.13
 * Time: 14:20
 * To change this template use File | Settings | File Templates.
 */
public class Settings {

    private final ConnectionInfo connectionInfo;
    private final List<Action> actions;

    public Settings(ConnectionInfo connectionInfo, List<Action> actions) {
        this.connectionInfo = connectionInfo;
        this.actions = actions;
    }

    public ConnectionInfo getConnectionInfo() {
        return connectionInfo;
    }

    public List<Action> getActions() {
        return new ArrayList<Action>(actions);
    }
}
