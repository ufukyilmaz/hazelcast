package com.hazelcast.core;

/**
 * Contains enterprise extensions for {@link IMap} interface.
 * Since 3.8 Continuous Query Cache featured was moved to open source, so this interface does not provide any
 * enterprise-specific additional features.
 *
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values
 * @see IMap
 * @since 3.5
 */
public interface IEnterpriseMap<K, V> extends IMap<K, V> {

}

