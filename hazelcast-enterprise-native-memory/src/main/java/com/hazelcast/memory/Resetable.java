package com.hazelcast.memory;

/**
 * Resets  the state of  object
 */
public interface Resetable {
    /**
     * Resets the state
     */
    void reset();
}
