package com.hazelcast.client.quorum.set;

import com.hazelcast.client.quorum.PartitionedClusterClients;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.ISet;
import com.hazelcast.enterprise.EnterpriseParametersRunnerFactory;
import com.hazelcast.quorum.set.SetReadQuorumTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.hazelcast.HDTestSupport.getHDConfig;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseParametersRunnerFactory.class)
@Category({QuickTest.class})
public class HDClientSetReadQuorumTest extends SetReadQuorumTest {

    private static PartitionedClusterClients CLIENTS;

    @BeforeClass
    public static void setUp() {
        TestHazelcastFactory factory = new TestHazelcastFactory();
        initTestEnvironment(getHDConfig(), factory);
        CLIENTS = new PartitionedClusterClients(CLUSTER, factory);
    }

    @AfterClass
    public static void tearDown() {
        shutdownTestEnvironment();
        CLIENTS.terminateAll();
    }

    protected ISet set(int index) {
        return CLIENTS.client(index).getSet(SET_NAME + quorumType.name());
    }

}
