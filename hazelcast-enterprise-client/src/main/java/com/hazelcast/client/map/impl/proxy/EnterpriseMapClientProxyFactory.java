package com.hazelcast.client.map.impl.proxy;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.ClientProxyFactory;
import com.hazelcast.config.NearCacheConfig;

import static com.hazelcast.map.impl.HDMapConfigValidator.checkHDConfig;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;

/**
 * Creates proxy instances for client according to given configuration.
 */
public class EnterpriseMapClientProxyFactory implements ClientProxyFactory {

    private final ClientConfig clientConfig;

    public EnterpriseMapClientProxyFactory(ClientConfig clientConfig) {
        this.clientConfig = clientConfig;
    }

    @Override
    public ClientProxy create(String id) {
        NearCacheConfig nearCacheConfig = clientConfig.getNearCacheConfig(id);
        if (nearCacheConfig != null) {
            checkHDConfig(nearCacheConfig, clientConfig.getNativeMemoryConfig());
            return new EnterpriseNearCachedClientMapProxyImpl(SERVICE_NAME, id);
        } else {
            return new EnterpriseClientMapProxyImpl(SERVICE_NAME, id);
        }
    }
}
