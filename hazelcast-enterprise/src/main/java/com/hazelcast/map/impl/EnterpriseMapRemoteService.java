package com.hazelcast.map.impl;

import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.map.impl.proxy.EnterpriseMapProxyImpl;
import com.hazelcast.map.impl.proxy.EnterpriseNearCachedMapProxyImpl;
import com.hazelcast.spi.NodeEngine;

import java.util.EnumSet;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static java.util.EnumSet.complementOf;

/**
 * Defines enterprise only remote service behavior for {@link MapService}
 *
 * @see MapService
 */
class EnterpriseMapRemoteService extends MapRemoteService {

    private static final EnumSet<EvictionPolicy> UNSUPPORTED_HD_NEAR_CACHE_EVICTION_POLICIES
            = EnumSet.of(EvictionPolicy.NONE, EvictionPolicy.RANDOM);

    private static final EnumSet<EvictionConfig.MaxSizePolicy> UNSUPPORTED_HD_NEAR_CACHE_MAXSIZE_POLICIES
            = EnumSet.of(EvictionConfig.MaxSizePolicy.ENTRY_COUNT);

    EnterpriseMapRemoteService(MapServiceContext mapServiceContext) {
        super(mapServiceContext);
    }

    @Override
    public DistributedObject createDistributedObject(String name) {
        MapConfig mapConfig = nodeEngine.getConfig().findMapConfig(name);
        MapService service = mapServiceContext.getService();
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();

        if (mapConfig.isNearCacheEnabled()) {
            checkPreconditions(mapConfig.getNearCacheConfig());

            return new EnterpriseNearCachedMapProxyImpl(name, service, nodeEngine);
        } else {
            return new EnterpriseMapProxyImpl(name, service, nodeEngine);
        }
    }

    /**
     * Checks preconditions to create a near-cached map proxy.
     *
     * @param nearCacheConfig the nearCacheConfig
     */
    protected void checkPreconditions(NearCacheConfig nearCacheConfig) {
        InMemoryFormat inMemoryFormat = nearCacheConfig.getInMemoryFormat();
        if (NATIVE != inMemoryFormat) {
            return;
        }

        EvictionConfig evictionConfig = nearCacheConfig.getEvictionConfig();

        EvictionPolicy evictionPolicy = evictionConfig.getEvictionPolicy();
        if (UNSUPPORTED_HD_NEAR_CACHE_EVICTION_POLICIES.contains(evictionPolicy)) {
            throw new IllegalArgumentException("Near-cache eviction policy " + evictionPolicy
                    + " cannot be used with NATIVE in memory format."
                    + " Supported eviction policies are : " + complementOf(UNSUPPORTED_HD_NEAR_CACHE_EVICTION_POLICIES));
        }

        EvictionConfig.MaxSizePolicy maximumSizePolicy = evictionConfig.getMaximumSizePolicy();
        if (UNSUPPORTED_HD_NEAR_CACHE_MAXSIZE_POLICIES.contains(maximumSizePolicy)) {
            throw new IllegalArgumentException("Near-cache maximum size policy " + maximumSizePolicy
                    + " cannot be used with NATIVE in memory format."
                    + " Supported maximum size policies are : " + complementOf(UNSUPPORTED_HD_NEAR_CACHE_MAXSIZE_POLICIES));
        }
    }

}
