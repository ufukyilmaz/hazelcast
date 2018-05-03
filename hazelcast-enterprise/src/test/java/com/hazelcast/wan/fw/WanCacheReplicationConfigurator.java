package com.hazelcast.wan.fw;

import com.hazelcast.cache.CacheMergePolicy;
import com.hazelcast.config.WanReplicationRef;

public class WanCacheReplicationConfigurator extends AbstractWanReplicationConfigurator<WanCacheReplicationConfigurator> {
    private final String cacheName;
    private Class<? extends CacheMergePolicy> mergePolicy;

    WanCacheReplicationConfigurator(Cluster sourceCluster, String cacheName) {
        super(sourceCluster);
        this.cacheName = cacheName;
    }
    public WanCacheReplicationConfigurator withMergePolicy(Class<? extends CacheMergePolicy> mergePolicy) {
        this.mergePolicy = mergePolicy;
        return this;
    }

    public void setup() {
        WanReplicationRef wanRef = new WanReplicationRef();
        wanRef.setName(wanReplication.getSetupName());
        wanRef.setMergePolicy(mergePolicy.getName());

        sourceCluster.config.getCacheConfig(cacheName).setWanReplicationRef(wanRef);
    }
}
