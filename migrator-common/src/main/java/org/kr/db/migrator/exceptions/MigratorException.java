package org.kr.db.migrator.exceptions;

/**
 * Created with IntelliJ IDEA.
 * User: I005144
 * Date: 01.07.13
 * Time: 10:55
 * To change this template use File | Settings | File Templates.
 */
public class MigratorException extends Exception {

    public MigratorException() {
        super();
    }

    public MigratorException(String message) {
        super(message);
    }

    public MigratorException(Throwable cause) {
        super(cause);
    }

    public MigratorException(String message, Throwable cause) {
        super(message, cause);
    }

}
