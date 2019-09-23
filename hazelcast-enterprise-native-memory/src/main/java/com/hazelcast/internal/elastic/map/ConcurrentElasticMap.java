package com.hazelcast.internal.elastic.map;

import java.util.concurrent.ConcurrentMap;

/**
 * @param <K> key type
 * @param <V> value type
 */
public interface ConcurrentElasticMap<K, V> extends ElasticMap<K, V>, ConcurrentMap<K, V> {
}
