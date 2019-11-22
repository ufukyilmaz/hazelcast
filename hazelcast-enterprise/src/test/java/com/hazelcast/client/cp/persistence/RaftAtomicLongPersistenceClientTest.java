package com.hazelcast.client.cp.persistence;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.internal.persistence.datastructures.RaftAtomicLongPersistenceTest;
import com.hazelcast.test.TestHazelcastInstanceFactory;

public class RaftAtomicLongPersistenceClientTest extends RaftAtomicLongPersistenceTest {

    private TestHazelcastFactory factory = new TestHazelcastFactory();

    @Override
    protected HazelcastInstance createProxyInstance(Config config) {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setRedoOperation(true);
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        return factory.newHazelcastClient(clientConfig);
    }

    @Override
    protected TestHazelcastInstanceFactory createTestFactory() {
        return factory;
    }
}
