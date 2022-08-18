package org.kr.db.migrator;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created with IntelliJ IDEA.
 * User: I005144
 * Date: 04.07.13
 * Time: 16:13
 * To change this template use File | Settings | File Templates.
 */
public class NamedThreadFactory implements ThreadFactory {

    private final String namePrefix;
    private final AtomicInteger threadCounter = new AtomicInteger(0);

    public static NamedThreadFactory getNamedThreadFactory(String namePrefix) {
        return new NamedThreadFactory(namePrefix);
    }

    private NamedThreadFactory(String namePrefix) {
        this.namePrefix = namePrefix;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(r, namePrefix + '-' + threadCounter.incrementAndGet());
        return t;
    }

}
