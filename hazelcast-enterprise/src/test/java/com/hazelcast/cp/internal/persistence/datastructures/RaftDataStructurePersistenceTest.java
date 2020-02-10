package com.hazelcast.cp.internal.persistence.datastructures;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.internal.persistence.PersistenceTestSupport;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.Accessors.getAddresses;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public abstract class RaftDataStructurePersistenceTest extends PersistenceTestSupport {

    protected Config config;
    protected HazelcastInstance[] instances;
    protected Address[] addresses;
    protected HazelcastInstance proxyInstance;
    protected final int commitIndexAdvanceCountToSnapshot = 100;

    @Before
    public void setUp() {
        config = createConfig(3, 3);
        config.getCPSubsystemConfig().getRaftAlgorithmConfig()
                .setCommitIndexAdvanceCountToSnapshot(commitIndexAdvanceCountToSnapshot);
        instances = factory.newInstances(config, 3);
        addresses = getAddresses(instances);
        proxyInstance = createProxyInstance(config);
    }

    protected HazelcastInstance createProxyInstance(Config config) {
        return factory.newHazelcastInstance(config);
    }

    protected void terminateMembers() {
        for (HazelcastInstance instance : instances) {
            instance.getLifecycleService().terminate();
        }
    }
}
