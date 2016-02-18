package com.hazelcast.elastic.map;

import java.util.concurrent.ConcurrentMap;

/**
 * @param <K> key type
 * @param <V> value type
 * @author mdogan 16/01/14
 */
public interface ConcurrentElasticMap<K, V> extends ElasticMap<K, V>, ConcurrentMap<K, V> {
}
