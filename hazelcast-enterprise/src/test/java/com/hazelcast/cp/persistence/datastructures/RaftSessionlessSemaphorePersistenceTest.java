package com.hazelcast.cp.persistence.datastructures;

import com.hazelcast.config.Config;
import com.hazelcast.config.cp.CPSemaphoreConfig;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RaftSessionlessSemaphorePersistenceTest extends AbstractRaftSemaphorePersistenceTest {

    @Override
    protected Config createConfig(int cpMemberCount, int groupSize) {
        Config config = super.createConfig(cpMemberCount, groupSize);
        CPSemaphoreConfig semaphoreConfig = new CPSemaphoreConfig("*", true);
        config.getCPSubsystemConfig().addSemaphoreConfig(semaphoreConfig);
        return config;
    }
}
