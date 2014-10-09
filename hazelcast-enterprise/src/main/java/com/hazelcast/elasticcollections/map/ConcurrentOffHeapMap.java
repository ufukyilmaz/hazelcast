package com.hazelcast.elasticcollections.map;

import java.util.concurrent.ConcurrentMap;

/**
 * @author mdogan 16/01/14
 */
public interface ConcurrentOffHeapMap<K, V> extends OffHeapMap<K, V>, ConcurrentMap<K, V> {
}
