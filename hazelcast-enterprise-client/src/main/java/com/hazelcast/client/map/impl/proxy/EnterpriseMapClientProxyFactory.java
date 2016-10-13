package com.hazelcast.client.map.impl.proxy;

import com.hazelcast.cache.impl.nearcache.NearCache;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.map.impl.nearcache.ClientHDNearCacheRegistry;
import com.hazelcast.client.spi.ClientExecutionService;
import com.hazelcast.client.spi.ClientPartitionService;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.ClientProxyFactory;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.map.impl.utils.Registry;
import com.hazelcast.spi.serialization.SerializationService;

import static com.hazelcast.map.impl.HDMapConfigValidator.checkHDConfig;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;

/**
 * Creates proxy instances for client according to given configuration.
 */
public class EnterpriseMapClientProxyFactory implements ClientProxyFactory {

    private final Registry<String, NearCache> hdNearCacheRegistry;
    private final ClientConfig clientConfig;

    public EnterpriseMapClientProxyFactory(ClientExecutionService executionService,
                                           SerializationService serializationService,
                                           ClientConfig clientConfig, ClientPartitionService partitionService) {
        this.clientConfig = clientConfig;

        this.hdNearCacheRegistry
                = new ClientHDNearCacheRegistry(executionService, serializationService, clientConfig, partitionService);
    }

    @Override
    public ClientProxy create(String id) {
        NearCacheConfig nearCacheConfig = clientConfig.getNearCacheConfig(id);
        if (nearCacheConfig != null) {
            checkHDConfig(nearCacheConfig, clientConfig.getNativeMemoryConfig());
            return new EnterpriseNearCachedClientMapProxyImpl(SERVICE_NAME, id, hdNearCacheRegistry);
        } else {
            return new EnterpriseClientMapProxyImpl(SERVICE_NAME, id);
        }
    }
}
