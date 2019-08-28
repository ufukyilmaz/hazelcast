package com.hazelcast.cp.persistence.datastructures;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.persistence.PersistenceTestSupport;
import org.junit.Before;

public abstract class RaftDataStructurePersistenceTestSupport extends PersistenceTestSupport {

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
