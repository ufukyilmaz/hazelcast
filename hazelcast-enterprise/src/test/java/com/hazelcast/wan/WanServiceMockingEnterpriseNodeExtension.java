package com.hazelcast.wan;

import com.hazelcast.instance.impl.EnterpriseNodeExtension;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.wan.impl.WanReplicationService;

public class WanServiceMockingEnterpriseNodeExtension extends EnterpriseNodeExtension {
    private final WanReplicationService wanReplicationService;

    public WanServiceMockingEnterpriseNodeExtension(Node node,
                                                    WanReplicationService wanReplicationService) {
        super(node);
        this.wanReplicationService = wanReplicationService;
    }

    @Override
    public <T> T createService(Class<T> clazz) {
        return clazz.isAssignableFrom(WanReplicationService.class)
                ? (T) wanReplicationService : super.createService(clazz);
    }
}
