package com.hazelcast.client.map.impl.proxy;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.impl.ClientProxyFactoryWithContext;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.map.impl.HDMapConfigValidator;
import com.hazelcast.map.impl.eviction.HotRestartEvictionHelper;
import com.hazelcast.spi.properties.HazelcastProperties;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;

/**
 * Creates map proxy instances for client according to given configuration.
 */
public class EnterpriseMapClientProxyFactory extends ClientProxyFactoryWithContext {

    private final ClientConfig clientConfig;
    private final HDMapConfigValidator hdMapConfigValidator;

    public EnterpriseMapClientProxyFactory(ClientConfig clientConfig, HazelcastProperties properties) {
        this.clientConfig = clientConfig;
        this.hdMapConfigValidator = new HDMapConfigValidator(new HotRestartEvictionHelper(properties));
    }

    @Override
    public ClientProxy create(String id, ClientContext context) {
        NearCacheConfig nearCacheConfig = clientConfig.getNearCacheConfig(id);
        if (nearCacheConfig != null) {
            hdMapConfigValidator.checkHDConfig(nearCacheConfig, clientConfig.getNativeMemoryConfig(), true);
            return new EnterpriseNearCachedClientMapProxyImpl(SERVICE_NAME, id, context);
        } else {
            return new EnterpriseClientMapProxyImpl(SERVICE_NAME, id, context);
        }
    }
}
