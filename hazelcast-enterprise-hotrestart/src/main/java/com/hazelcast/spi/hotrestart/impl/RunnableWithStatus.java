package com.hazelcast.spi.hotrestart.impl;

/**
 * Runnable with a "submitterCanProceed" flag.
 */
abstract class RunnableWithStatus implements Runnable {
    volatile boolean submitterCanProceed;

    RunnableWithStatus(boolean submitterCanProceed) {
        this.submitterCanProceed = submitterCanProceed;
    }
}
