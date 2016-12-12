package com.hazelcast.map.impl.proxy;

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.IEnterpriseMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.spi.NodeEngine;

/**
 * A server-side {@code IEnterpriseMap} implementation which is fronted by a Near Cache.
 *
 * @param <K> the key type for this {@code IMap} proxy.
 * @param <V> the value type for this {@code IMap} proxy.
 */
public class EnterpriseNearCachedMapProxyImpl<K, V> extends NearCachedMapProxyImpl<K, V> implements IEnterpriseMap<K, V> {

    public EnterpriseNearCachedMapProxyImpl(String name, MapService mapService, NodeEngine nodeEngine, MapConfig mapConfig) {
        super(name, mapService, nodeEngine, mapConfig);
    }

}
