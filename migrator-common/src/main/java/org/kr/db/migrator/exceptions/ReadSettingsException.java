package org.kr.db.migrator.exceptions;

/**
 * Created with IntelliJ IDEA.
 * User: I005144
 * Date: 01.07.13
 * Time: 10:55
 * To change this template use File | Settings | File Templates.
 */
public class ReadSettingsException extends MigratorException {

    public ReadSettingsException() {
        super();
    }

    public ReadSettingsException(String message) {
        super(message);
    }

    public ReadSettingsException(Throwable cause) {
        super(cause);
    }

    public ReadSettingsException(String message, Throwable cause) {
        super(message, cause);
    }

}
