package com.hazelcast.client.cp.persistence;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.internal.persistence.datastructures.RaftSessionAwareSemaphorePersistenceTest;
import com.hazelcast.test.TestHazelcastInstanceFactory;

public class RaftSessionAwareSemaphorePersistenceClientTest extends RaftSessionAwareSemaphorePersistenceTest {

    private TestHazelcastFactory factory = new TestHazelcastFactory();

    @Override
    protected HazelcastInstance createProxyInstance(Config config) {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setRedoOperation(true);
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setFailOnMaxBackoff(false);
        return factory.newHazelcastClient(clientConfig);
    }

    @Override
    protected TestHazelcastInstanceFactory createTestFactory() {
        return factory;
    }
}
