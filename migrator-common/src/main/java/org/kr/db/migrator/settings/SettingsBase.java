package org.kr.db.migrator.settings;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: I005144
 * Date: 01.07.13
 * Time: 11:44
 * To change this template use File | Settings | File Templates.
 */
public class SettingsBase {

    private ConnectionInfo connectionInfo;
    private List<? extends QueryBase> queryList;

    public SettingsBase(ConnectionInfo connectionInfo, List<? extends QueryBase> queryList) {
        this.connectionInfo = connectionInfo;
        this.queryList = queryList;
    }

    public ConnectionInfo getConnectionInfo() {
        return connectionInfo;
    }

    public List<? extends QueryBase> getQueryList() {
        return queryList;
    }

}
