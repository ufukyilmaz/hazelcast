package com.hazelcast.enterprise.wan.impl;

/**
 * Interface to be implemented by the entries intended to be stored in a
 * {@link TwoPhasedLinkedQueue}. Since the entries in that queue may need
 * finalization, through the {@link Finalizer} interface.
 *
 * @see Finalizer
 * @see Finalizable
 * @see TwoPhasedLinkedQueue
 */
public interface FinalizerAware {
    /**
     * Sets the {@link Finalizer} instance to be used for finalizing the entry.
     *
     * @param finalizer The finalizer to be used.
     */
    void setFinalizer(Finalizer finalizer);
}
