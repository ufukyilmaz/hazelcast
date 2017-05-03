package com.hazelcast.client.map.impl.proxy;

import com.hazelcast.client.proxy.NearCachedClientMapProxy;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.core.IEnterpriseMap;

/**
 * Contains enterprise-part extensions to {@code ClientMapProxy}.
 * Since 3.8, continuous query cache has been moved to open source, so this class is empty.
 *
 * @param <K> the key type for this {@code IMap} proxy.
 * @param <V> the value type for this {@code IMap} proxy.
 */
public class EnterpriseNearCachedClientMapProxyImpl<K, V> extends NearCachedClientMapProxy<K, V> implements IEnterpriseMap<K, V> {

    EnterpriseNearCachedClientMapProxyImpl(String serviceName, String name, ClientContext context) {
        super(serviceName, name, context);
    }
}
