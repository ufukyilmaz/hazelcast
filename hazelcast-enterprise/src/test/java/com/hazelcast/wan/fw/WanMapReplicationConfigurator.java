package com.hazelcast.wan.fw;

import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.map.merge.PassThroughMergePolicy;

public class WanMapReplicationConfigurator extends AbstractWanReplicationConfigurator<WanMapReplicationConfigurator> {
    private final String mapName;
    private Class<? extends MapMergePolicy> mergePolicy = PassThroughMergePolicy.class;

    WanMapReplicationConfigurator(Cluster sourceCluster, String mapName) {
        super(sourceCluster);
        this.mapName = mapName;
    }

    public WanMapReplicationConfigurator withMergePolicy(Class<? extends MapMergePolicy> mergePolicy) {
        this.mergePolicy = mergePolicy;
        return this;
    }

    public void setup() {
        WanReplicationRef wanRef = new WanReplicationRef();
        wanRef.setName(wanReplication.getSetupName());
        wanRef.setMergePolicy(mergePolicy.getName());

        sourceCluster.config.getMapConfig(mapName).setWanReplicationRef(wanRef);
    }

}
