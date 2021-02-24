package com.hazelcast.enterprise.wan.impl;

/**
 * Interface for the finalization logic to be used for the {@link Finalizable}
 * objects. This interface is introduced to detach the actual finalization logic
 * and the objects to be finalized in order to keep the model objects as lazily
 * coupled to finalization as possible.
 * <p/>
 * Finalization is the process taking place after a WAN event has been acknowledged
 * by the target cluster like removing its traces from the data structure that stores
 * it until the finalization is done.
 *
 * @see Finalizable
 * @see FinalizerAware
 */
@FunctionalInterface
public interface Finalizer {
    /**
     * Finalizes the implementor object.
     */
    void doFinalize();
}
