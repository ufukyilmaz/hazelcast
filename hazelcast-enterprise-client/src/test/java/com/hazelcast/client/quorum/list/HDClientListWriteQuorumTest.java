package com.hazelcast.client.quorum.list;

import com.hazelcast.client.quorum.PartitionedClusterClients;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.IList;
import com.hazelcast.enterprise.EnterpriseParametersRunnerFactory;
import com.hazelcast.quorum.list.ListWriteQuorumTest;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.hazelcast.HDTestSupport.getHDConfig;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class HDClientListWriteQuorumTest extends ListWriteQuorumTest {

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

    protected IList list(int index) {
        return CLIENTS.client(index).getList(LIST_NAME + quorumType.name());
    }

}
