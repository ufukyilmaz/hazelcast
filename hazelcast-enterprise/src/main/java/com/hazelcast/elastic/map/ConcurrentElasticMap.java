package com.hazelcast.elastic.map;

import java.util.concurrent.ConcurrentMap;

/**
 * @author mdogan 16/01/14
 */
public interface ConcurrentElasticMap<K, V> extends ElasticMap<K, V>, ConcurrentMap<K, V> {
}
