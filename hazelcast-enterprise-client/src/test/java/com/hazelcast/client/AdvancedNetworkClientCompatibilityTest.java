package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.AdvancedNetworkConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.ServerSocketEndpointConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static org.junit.Assert.assertEquals;

// Smoke test client compatibility when Hazelcast 3.12 is configured with multiple endpoints
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class AdvancedNetworkClientCompatibilityTest {

    private static final int CLUSTER_SIZE = 3;

    private CompatibilityTestHazelcastFactory factory;
    private HazelcastInstance[] members;

    @Before
    public void setup() {
        factory = new CompatibilityTestHazelcastFactory();
        members = new HazelcastInstance[CLUSTER_SIZE];
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            members[i] = Hazelcast.newHazelcastInstance(getMultiEndpointConfig());
        }
    }

    @After
    public void tearDown() {
        factory.terminateAll();
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            members[i].getLifecycleService().terminate();
        }
    }

    @Test
    public void testClientsCompatible_whenMultipleEndpointsConfigured() {
        String[] clientVersions = new String[] {"3.11.2", "3.10.6", "3.9.4", "3.8.6", "3.7.8", "3.6.8"};
        HazelcastInstance[] clients = new HazelcastInstance[clientVersions.length];
        for (int i = 0; i < clientVersions.length; i++) {
            clients[i] = factory.newHazelcastClient(clientVersions[i], getClientConfig());
        }

        for (int i = 0; i < clientVersions.length; i++) {
            IAtomicLong atomicLong = clients[i].getAtomicLong(clientVersions[i]);

            for (int j = 0; j < 1000; j++) {
                atomicLong.incrementAndGet();
            }
            assertEquals(1000, atomicLong.get());
        }
    }

    private Config getMultiEndpointConfig() {
        Config config = smallInstanceConfig();
        AdvancedNetworkConfig netConfig = config.getAdvancedNetworkConfig();
        netConfig.setEnabled(true);

        // joiner
        netConfig.getJoin().getMulticastConfig().setEnabled(false);
        netConfig.getJoin().getTcpIpConfig().setEnabled(true).addMember("127.0.0.1");

        // server sockets: default member config (5701 + auto-increment), clients on 9000 + auto-increment
        netConfig.setClientEndpointConfig(new ServerSocketEndpointConfig().setPort(9000));

        return config;
    }

    private ClientConfig getClientConfig() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().addAddress("127.0.0.1:9000");
        return clientConfig;
    }
}
