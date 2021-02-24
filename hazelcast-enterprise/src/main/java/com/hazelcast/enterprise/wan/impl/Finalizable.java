package com.hazelcast.enterprise.wan.impl;

/**
 * Interface that the classes intended to be finalized needs to implement.
 * <p/>
 * Finalization is the process taking place after a WAN event has been acknowledged
 * by the target cluster like removing its traces from the data structure stores
 * it until the finalization is done.
 *
 * @see Finalizer
 * @see FinalizerAware
 */
public interface Finalizable {
    /**
     * Performs the finalization on the object.
     */
    void doFinalize();
}
