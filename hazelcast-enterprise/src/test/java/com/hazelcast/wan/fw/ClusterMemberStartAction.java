package com.hazelcast.wan.fw;

import com.hazelcast.core.HazelcastInstance;

public interface ClusterMemberStartAction {
    void onMemberStarted(HazelcastInstance instance);
}
