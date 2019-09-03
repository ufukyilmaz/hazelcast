package com.hazelcast.splitbrainprotection;

import com.hazelcast.config.Config;
import com.hazelcast.config.SplitBrainProtectionConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.transaction.TransactionContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.hazelcast.internal.cluster.Versions.CURRENT_CLUSTER_VERSION;
import static com.hazelcast.internal.cluster.Versions.PREVIOUS_CLUSTER_VERSION;
import static org.junit.Assert.fail;

// Ensures a data structure which was protected by split brain protection in previous version
// is still protected once cluster is upgraded.
public abstract class AbstractSplitBrainProtectionCompatibilityTest extends HazelcastTestSupport {

    private static final String[] VERSIONS = new String[] {
            PREVIOUS_CLUSTER_VERSION.toString(),
            PREVIOUS_CLUSTER_VERSION.toString(),
            CURRENT_CLUSTER_VERSION.toString(),
            CURRENT_CLUSTER_VERSION.toString(),
    };

    protected String name;
    private CompatibilityTestHazelcastInstanceFactory factory;
    private HazelcastInstance[] members;

    @Before
    public void setup() {
        name = randomString();
        members = new HazelcastInstance[VERSIONS.length];
        factory = new CompatibilityTestHazelcastInstanceFactory(VERSIONS);
        // start 2 previous version members
        members[0] = factory.newHazelcastInstance(getSplitBrainProtectedConfig());
        members[1] = factory.newHazelcastInstance(getSplitBrainProtectedConfig());
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test
    public void test_structureProtected_whenPreviousAndCurrentClusterVersion() {
        prepareDataStructure(members[0]);

        members[2] = factory.newHazelcastInstance(getSplitBrainProtectedConfig());
        assertClusterSize(3, members[2]);

        members[0].shutdown();
        waitAllForSafeState(members[2]);

        // assert split brain protection works on previous cluster version
        assertOperations_whileSplitBrainProtectionPresent(members[2]);

        // shutdown one more member - split brain protection size requirement is not met
        members[1].shutdown();
        waitAllForSafeState(members[2]);

        ClusterService clusterService = getClusterService(members[2]);
        clusterService.changeClusterVersion(CURRENT_CLUSTER_VERSION);

        try {
            assertOperations_whileSplitBrainProtectionAbsent(members[2]);
            fail("Split brain protection is absent, operation should have failed with SplitBrainProtectionException");
        } catch (SplitBrainProtectionException e) {
            // ignore
        }

        members[3] = factory.newHazelcastInstance(getSplitBrainProtectedConfig());
        waitAllForSafeState(members[2], members[3]);

        // assert split brain protection works on current cluster version
        assertOperations_whileSplitBrainProtectionPresent(members[2]);
    }

    protected Config getConfigWithSplitBrainProtection() {
        Config config = new Config();
        config.addSplitBrainProtectionConfig(
                SplitBrainProtectionConfig.newProbabilisticSplitBrainProtectionConfigBuilder("pq", 2)
                            .build()
        );
        return config;
    }

    // initialization on previous version member
    protected abstract void prepareDataStructure(HazelcastInstance previousVersionMember);

    // execute operation which requires split brain protection while split brain protection is absent
    // a SplitBrainProtectionException is expected to be thrown
    protected abstract void assertOperations_whileSplitBrainProtectionAbsent(HazelcastInstance member);

    // execute operation which requires split brain protection while split brain protection is present
    protected abstract void assertOperations_whileSplitBrainProtectionPresent(HazelcastInstance member);

    protected abstract Config getSplitBrainProtectedConfig();

    protected TransactionContext getTransactionalContext(HazelcastInstance member) {
        return member.newTransactionContext();
    }
}
