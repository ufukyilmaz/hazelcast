package com.hazelcast.quorum;

import com.hazelcast.config.Config;
import com.hazelcast.config.QuorumConfig;
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

// RU_COMPAT_3_9
// Ensures a data structure which was not previously protected by quorum (in 3.9)
// becomes protected once cluster is upgraded to 3.10 (member codebase & cluster version),
// assuming 3.10 members configure these structures with split-brain protection.
public abstract class AbstractQuorumCompatibilityTest extends HazelcastTestSupport {

    private static final String[] VERSIONS = new String[] {
            PREVIOUS_CLUSTER_VERSION.toString(),
            PREVIOUS_CLUSTER_VERSION.toString(),
            CURRENT_CLUSTER_VERSION.toString(),
            CURRENT_CLUSTER_VERSION.toString()
    };

    protected String name;
    private CompatibilityTestHazelcastInstanceFactory factory;
    private HazelcastInstance[] members;

    @Before
    public void setup() {
        name = randomString();
        members = new HazelcastInstance[VERSIONS.length];
        factory = new CompatibilityTestHazelcastInstanceFactory(VERSIONS);
        // start 2*3.9 members
        members[0] = factory.newHazelcastInstance();
        members[1] = factory.newHazelcastInstance();
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test
    public void test_structureBecomesProtected_withNewQuorumConfig_under_3_10() {
        prepareDataStructure(members[0]);

        members[2] = factory.newHazelcastInstance(getQuorumProtectedConfig());
        assertClusterSize(3, members[2]);

        members[0].shutdown();
        sleepSeconds(5);
        members[1].shutdown();
        waitAllForSafeState(members[2]);

        assertOnCurrentMembers_whilePreviousClusterVersion(members[2]);

        ClusterService clusterService = getClusterService(members[2]);
        clusterService.changeClusterVersion(CURRENT_CLUSTER_VERSION);

        try {
            assertOnCurrent_whileQuorumAbsent(members[2]);
            fail("Quorum is absent, operation should have failed with QuorumException");
        } catch (QuorumException e) {
            // ignore
        }

        members[3] = factory.newHazelcastInstance(getQuorumProtectedConfig());
        waitAllForSafeState(members[2], members[3]);

        assertOnCurrent_whileQuorumPresent(members[2]);
    }

    protected Config getConfigWithQuorum() {
        Config config = new Config();
        config.addQuorumConfig(
                QuorumConfig.newProbabilisticQuorumConfigBuilder("pq", 2)
                            .build()
        );
        return config;
    }

    // initialization on 3.9 member
    protected abstract void prepareDataStructure(HazelcastInstance previousVersionMember);

    // assert operations which require a quorum in 3.10 but do not require quorum in 3.9
    // are executed as expected on a 3.10 member with 3.9 cluster version
    protected abstract void assertOnCurrentMembers_whilePreviousClusterVersion(HazelcastInstance member);

    // execute operation which requires quorum on 3.10 while quorum is absent
    // a QuorumException is expected to be thrown
    protected abstract void assertOnCurrent_whileQuorumAbsent(HazelcastInstance member);

    // execute operation which requires quorum on 3.10 while quorum is present
    protected abstract void assertOnCurrent_whileQuorumPresent(HazelcastInstance member);

    protected abstract Config getQuorumProtectedConfig();

    protected TransactionContext getTransactionalContext(HazelcastInstance member) {
        return member.newTransactionContext();
    }
}
