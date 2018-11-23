package com.hazelcast.client.map.impl.proxy;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.impl.ClientProxyFactoryWithContext;
import com.hazelcast.config.NearCacheConfig;

import static com.hazelcast.config.NearCacheConfigAccessor.initDefaultMaxSizeForOnHeapMaps;
import static com.hazelcast.internal.config.ConfigValidator.checkNearCacheConfig;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;

/**
 * Creates map proxy instances for client according to given configuration.
 */
public class EnterpriseMapClientProxyFactory extends ClientProxyFactoryWithContext {

    private final ClientConfig clientConfig;

    public EnterpriseMapClientProxyFactory(ClientConfig clientConfig) {
        this.clientConfig = clientConfig;
    }

    @Override
    public ClientProxy create(String id, ClientContext context) {
        NearCacheConfig nearCacheConfig = clientConfig.getNearCacheConfig(id);
        if (nearCacheConfig != null) {
            checkNearCacheConfig(id, nearCacheConfig, clientConfig.getNativeMemoryConfig(), true);
            initDefaultMaxSizeForOnHeapMaps(nearCacheConfig);
            return new EnterpriseNearCachedClientMapProxyImpl(SERVICE_NAME, id, context);
        } else {
            return new EnterpriseClientMapProxyImpl(SERVICE_NAME, id, context);
        }
    }
}
