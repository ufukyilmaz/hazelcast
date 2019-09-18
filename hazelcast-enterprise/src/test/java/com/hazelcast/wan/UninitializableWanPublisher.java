package com.hazelcast.wan;

import com.hazelcast.config.AbstractWanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;

/**
 * WAN publisher implementation that throws an exception on initialization.
 */
public class UninitializableWanPublisher implements WanReplicationPublisher<Object> {

    public UninitializableWanPublisher() {
    }

    @Override
    public void init(WanReplicationConfig wanReplicationConfig, AbstractWanPublisherConfig pc) {
        throw new UnsupportedOperationException("This publisher cannot be initialized!");
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void publishReplicationEvent(WanReplicationEvent event) {
    }

    @Override
    public void publishReplicationEventBackup(WanReplicationEvent event) {
    }

    @Override
    public void republishReplicationEvent(WanReplicationEvent event) {
    }

    @Override
    public void doPrepublicationChecks() {
    }
}
