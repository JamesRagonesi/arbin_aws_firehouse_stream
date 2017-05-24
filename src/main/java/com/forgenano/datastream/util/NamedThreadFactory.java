package com.forgenano.datastream.util;

import java.util.concurrent.ThreadFactory;

/**
 * Created by michael on 5/8/17.
 */
public class NamedThreadFactory implements ThreadFactory {

    public static final NamedThreadFactory NewNamedDaemonThreadFactory(String threadNamePrefix) {
        return new NamedThreadFactory(threadNamePrefix, true);
    }

    public static final NamedThreadFactory NewNamedNonDaemonThreadFactory(String threadNamePrefix) {
        return new NamedThreadFactory(threadNamePrefix, false);
    }

    private String threadNamePrefix;
    private boolean daemonThread;

    private NamedThreadFactory(String threadNamePrefix, boolean daemonThread) {
        this.threadNamePrefix = threadNamePrefix;
        this.daemonThread = daemonThread;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread newThread = new Thread(r);

        newThread.setDaemon(this.daemonThread);
        newThread.setName(this.threadNamePrefix + "-" + newThread.getId());

        return newThread;
    }
}
