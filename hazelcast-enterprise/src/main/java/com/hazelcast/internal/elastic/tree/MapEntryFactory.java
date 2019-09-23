package com.hazelcast.internal.elastic.tree;

import com.hazelcast.nio.serialization.Data;

import java.util.Map;

/**
 * Factory responsible for creation of Map.Entry instances
 *
 * @param <T> concrete type of the Map.Entry
 */
public interface MapEntryFactory<T extends Map.Entry> {

    /**
     * Creates an instance of a Map.Entry implementation
     *
     * @param key given key
     * @param value given value
     * @return populated instance of a Map.Entry object containing the given key and value
     */
    T create(Data key, Data value);

}
