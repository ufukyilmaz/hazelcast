package com.hazelcast.memory;

/**
* @author mdogan 12/04/14
*/
public interface LibMalloc {

    long malloc(long size);

    void free(long address);

}
