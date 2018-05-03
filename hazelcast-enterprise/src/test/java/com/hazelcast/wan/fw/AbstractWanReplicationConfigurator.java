package com.hazelcast.wan.fw;

class AbstractWanReplicationConfigurator<T extends AbstractWanReplicationConfigurator> {
    protected final Cluster sourceCluster;
    protected WanReplication wanReplication;

    AbstractWanReplicationConfigurator(Cluster sourceCluster) {
        this.sourceCluster = sourceCluster;
    }

    @SuppressWarnings("unchecked")
    public T withReplication(WanReplication wanReplication) {
        this.wanReplication = wanReplication;
        return (T) this;
    }

}
