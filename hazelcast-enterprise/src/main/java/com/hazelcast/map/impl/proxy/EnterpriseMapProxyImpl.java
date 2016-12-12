package com.hazelcast.map.impl.proxy;

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.IEnterpriseMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.spi.NodeEngine;

/**
 * Proxy implementation of {@link com.hazelcast.core.IMap} interface
 * which includes enterprise version specific extensions.
 *
 * @param <K> the key type of map.
 * @param <V> the value type of map.
 */
public class EnterpriseMapProxyImpl<K, V> extends MapProxyImpl<K, V> implements IEnterpriseMap<K, V> {

    public EnterpriseMapProxyImpl(String name, MapService mapService, NodeEngine nodeEngine, MapConfig mapConfig) {
        super(name, mapService, nodeEngine, mapConfig);
    }

}
