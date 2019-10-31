package com.hazelcast.cp.persistence.datastructures;

import com.hazelcast.config.Config;
import com.hazelcast.config.cp.SemaphoreConfig;

public class RaftSessionlessSemaphorePersistenceTest extends AbstractRaftSemaphorePersistenceTest {

    @Override
    protected Config createConfig(int cpMemberCount, int groupSize) {
        Config config = super.createConfig(cpMemberCount, groupSize);
        SemaphoreConfig semaphoreConfig = new SemaphoreConfig("*", true, 0);
        config.getCPSubsystemConfig().addSemaphoreConfig(semaphoreConfig);
        return config;
    }
}
