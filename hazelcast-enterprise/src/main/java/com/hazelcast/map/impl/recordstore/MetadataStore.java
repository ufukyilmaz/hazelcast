package com.hazelcast.map.impl.recordstore;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.query.impl.Metadata;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class MetadataStore {

    private final ConcurrentMap<Data, Metadata> store;

    public MetadataStore() {
        this.store = new ConcurrentHashMap<>();
    }

    public Metadata get(Data key) {
        return store.get(key);
    }

    public void set(Data key, Metadata metadata) {
        store.put(key, metadata);
    }

    public void remove(Data key) {
        store.remove(key);
    }

    public void clear() {
        store.clear();
    }
}
