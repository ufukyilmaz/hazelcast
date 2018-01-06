package com.hazelcast.client.quorum.cache;

import com.hazelcast.cache.ICache;
import com.hazelcast.client.quorum.PartitionedClusterClients;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.enterprise.EnterpriseParametersRunnerFactory;
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
public class HDClientMapQuorumReadTest extends ClientCacheQuorumReadTest {

    private static PartitionedClusterClients clients;

    @BeforeClass
    public static void setUp() {
        TestHazelcastFactory factory = new TestHazelcastFactory();
        initTestEnvironment(getHDConfig(), factory);
        clients = new PartitionedClusterClients(cluster, factory);
    }

    @AfterClass
    public static void tearDown() {
        shutdownTestEnvironment();
        clients.terminateAll();
    }

    @Override
    protected ICache<Integer, String> cache(int index) {
        return clients.client(index).getCacheManager().getCache(CACHE_NAME + quorumType.name());
    }

}
