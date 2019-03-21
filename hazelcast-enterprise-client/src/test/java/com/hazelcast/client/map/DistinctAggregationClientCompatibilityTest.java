package com.hazelcast.client.map;

import com.hazelcast.aggregation.Aggregators;
import com.hazelcast.aggregation.impl.CanonicalizingHashSet;
import com.hazelcast.client.CompatibilityTestHazelcastFactory;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory.CURRENT_VERSION;
import static com.hazelcast.test.HazelcastTestSupport.assertInstanceOf;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class DistinctAggregationClientCompatibilityTest {

    private static final int CLUSTER_SIZE = 2;

    private CompatibilityTestHazelcastFactory factory;
    private HazelcastInstance[] members;

    @SuppressWarnings("unchecked")
    @Before
    public void setup() {
        factory = new CompatibilityTestHazelcastFactory();
        members = new HazelcastInstance[CLUSTER_SIZE];
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            members[i] = Hazelcast.newHazelcastInstance(getConfig());
        }

        IMap map = members[0].getMap("test");
        for (int i = 0; i < 1000; ++i) {
            map.put(i, i % 2 == 0 ? i : (double) i);
        }
    }

    @After
    public void tearDown() {
        factory.terminateAll();
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            members[i].getLifecycleService().terminate();
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testClientsAreCompatible() {
        String[] clientVersions = new String[]{CURRENT_VERSION, "3.11.2", "3.10.6"};
        HazelcastInstance[] clients = new HazelcastInstance[clientVersions.length];
        for (int i = 0; i < clientVersions.length; i++) {
            clients[i] = factory.newHazelcastClient(clientVersions[i], getClientConfig());
        }

        for (int i = 0; i < clientVersions.length; i++) {
            IMap map = clients[i].getMap("test");

            Set values = (Set) map.aggregate(Aggregators.distinct());

            assertEquals(1000, values.size());
            if (clientVersions[i].equals(CURRENT_VERSION)) {
                assertInstanceOf(CanonicalizingHashSet.class, values);
                for (int j = 0; j < 1000; ++j) {
                    assertTrue(values.contains(j));
                    assertTrue(values.contains((double) j));
                }
            } else {
                assertInstanceOf(HashSet.class, values);
                for (int j = 0; j < 1000; ++j) {
                    assertTrue(values.contains(j % 2 == 0 ? j : (double) j));
                }
            }
        }
    }

    private Config getConfig() {
        Config config = smallInstanceConfig();
        NetworkConfig netConfig = config.getNetworkConfig();
        netConfig.getJoin().getMulticastConfig().setEnabled(false);
        netConfig.getJoin().getTcpIpConfig().setEnabled(true).addMember("127.0.0.1");
        return config;
    }

    private ClientConfig getClientConfig() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().addAddress("127.0.0.1:5701");
        return clientConfig;
    }

}
