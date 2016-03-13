package com.hazelcast.spi.impl.memory;

/**
* @author mdogan 12/04/14
*/
public interface LibMalloc {

    /**
     * NULL pointer address.
     */
    long NULL_ADDRESS = 0L;

    long malloc(long size);

    long realloc(long address, long size);

    void free(long address);

}
