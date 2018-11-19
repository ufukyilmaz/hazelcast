package com.hazelcast.map.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.map.impl.eviction.HotRestartEvictionHelper;
import com.hazelcast.map.impl.proxy.EnterpriseMapProxyImpl;
import com.hazelcast.map.impl.proxy.EnterpriseNearCachedMapProxyImpl;
import com.hazelcast.map.merge.MergePolicyProvider;

import static com.hazelcast.config.NearCacheConfigAccessor.initDefaultMaxSizeForOnHeapMaps;
import static com.hazelcast.internal.config.ConfigValidator.checkMapConfig;
import static com.hazelcast.internal.config.ConfigValidator.checkNearCacheConfig;
import static com.hazelcast.internal.config.MergePolicyValidator.checkMergePolicySupportsInMemoryFormat;

/**
 * Defines enterprise only remote service behavior for {@link MapService}.
 *
 * @see MapService
 */
class EnterpriseMapRemoteService extends MapRemoteService {

    private final HDMapConfigValidator hdMapConfigValidator;

    EnterpriseMapRemoteService(MapServiceContext mapServiceContext) {
        super(mapServiceContext);
        HotRestartEvictionHelper hotRestartEvictionHelper = new HotRestartEvictionHelper(nodeEngine.getProperties());
        hdMapConfigValidator = new HDMapConfigValidator(hotRestartEvictionHelper);
    }

    @Override
    public DistributedObject createDistributedObject(String name) {
        Config config = nodeEngine.getConfig();
        MapConfig mapConfig = config.findMapConfig(name);
        NativeMemoryConfig nativeMemoryConfig = config.getNativeMemoryConfig();

        MergePolicyProvider mergePolicyProvider = mapServiceContext.getMergePolicyProvider();

        checkMapConfig(mapConfig, mergePolicyProvider);
        hdMapConfigValidator.checkHDConfig(mapConfig, nativeMemoryConfig);

        Object mergePolicy = mergePolicyProvider.getMergePolicy(mapConfig.getMergePolicyConfig().getPolicy());
        checkMergePolicySupportsInMemoryFormat(name, mergePolicy, mapConfig.getInMemoryFormat(),
                true, nodeEngine.getLogger(getClass()));

        if (mapConfig.isNearCacheEnabled()) {
            checkNearCacheConfig(name, mapConfig.getNearCacheConfig(), nativeMemoryConfig, false);
            initDefaultMaxSizeForOnHeapMaps(mapConfig.getNearCacheConfig());
            return new EnterpriseNearCachedMapProxyImpl(name, mapServiceContext.getService(), nodeEngine, mapConfig);
        } else {
            return new EnterpriseMapProxyImpl(name, mapServiceContext.getService(), nodeEngine, mapConfig);
        }
    }
}
