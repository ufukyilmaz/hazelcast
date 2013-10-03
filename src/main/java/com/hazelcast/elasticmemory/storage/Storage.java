package com.hazelcast.elasticmemory.storage;


import com.hazelcast.nio.serialization.Data;

public interface Storage {

    public final static int _1K = 1024;
    public final static int _1M = _1K * _1K;

    DataRef put(int hash, Data data);

    Data get(int hash, DataRef entry);

    void remove(int hash, DataRef entry);

    void destroy();
}
