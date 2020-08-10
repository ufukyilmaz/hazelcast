package com.hazelcast.wan;

import static com.hazelcast.auditlog.AuditlogTypeIds.WAN_ADD_CONFIG;
import static com.hazelcast.auditlog.AuditlogTypeIds.WAN_SYNC;
import static com.hazelcast.test.Accessors.getAuditlogService;
import static com.hazelcast.wan.fw.Cluster.clusterA;
import static com.hazelcast.wan.fw.Cluster.clusterB;
import static com.hazelcast.wan.fw.WanMapTestSupport.fillMap;
import static com.hazelcast.wan.fw.WanMapTestSupport.verifyMapReplicated;
import static com.hazelcast.wan.fw.WanReplication.replicate;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.enterprise.wan.impl.replication.WanBatchPublisher;
import com.hazelcast.security.auditlog.TestAuditlogService;
import com.hazelcast.security.auditlog.TestAuditlogServiceFactory;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.test.ChangeLoggingRule;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.fw.Cluster;
import com.hazelcast.wan.fw.WanReplication;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class WanAuditlogTest extends HazelcastTestSupport {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @ClassRule
    public static ChangeLoggingRule changeLoggingRule = new ChangeLoggingRule("log4j2-debug.xml");

    private static final String MAP_NAME = "map";
    private static final String REPLICATION_NAME = "wanReplication";

    private Cluster clusterA;
    private Cluster clusterB;

    private TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

    @After
    public void cleanup() {
        factory.shutdownAll();
    }

    @Before
    public void setup() {
        Config config = smallInstanceConfig();
        config.getAuditlogConfig()
            .setEnabled(true)
            .setFactoryClassName(TestAuditlogServiceFactory.class.getName());
        clusterA = clusterA(factory, 2, () -> config).setup();
        clusterB = clusterB(factory, 2).setup();
    }

    @Test
    public void testAddConfig() {
        WanReplication toBReplication = replicate()
                .to(clusterB)
                .withSetupName(REPLICATION_NAME)
                .setup();

        clusterA.startCluster();
        clearAuditlogOnMembers(clusterA);
        clusterA.addWanReplication(toBReplication);
        boolean eventPresent = false;
        for (HazelcastInstance hz : clusterA.getMembers()) {
            TestAuditlogService auditlog = (TestAuditlogService) getAuditlogService(hz);
            eventPresent |= auditlog.hasEvent(WAN_ADD_CONFIG);
            auditlog.getEventQueue().clear();
        }
        assertTrue(eventPresent);
    }

    @Test
    public void testReplicate() {
        WanReplication toBReplication = replicate()
                .to(clusterB)
                .withSetupName(REPLICATION_NAME)
                .withWanPublisher(WanBatchPublisher.class)
                .withInitialPublisherState(WanPublisherState.STOPPED)
                .setup();

        clusterA.replicateMap(MAP_NAME)
            .withReplication(toBReplication)
            .withMergePolicy(PassThroughMergePolicy.class)
            .setup();

        clusterA.startCluster();
        clusterB.startCluster();
        clusterA.addWanReplication(toBReplication);
        fillMap(clusterA, MAP_NAME, 0, 1000);
        assertTrue(clusterB.getAMember().getMap(MAP_NAME).isEmpty());
        clearAuditlogOnMembers(clusterA);

        clusterA.syncMap(toBReplication, MAP_NAME);

        verifyMapReplicated(clusterA, clusterB, MAP_NAME);

        // verify that a cluster A member has WAN_SYNC event in its auditlog
        assertWanSyncEventPresent();

        clearAuditlogOnMembers(clusterA);

        clusterA.syncAllMaps(toBReplication);
        assertWanSyncEventPresent();
    }

    private void assertWanSyncEventPresent() {
        assertTrue(
                Arrays.stream(clusterA.getMembers())
                .map(hz -> (TestAuditlogService) getAuditlogService(hz))
                .anyMatch(al -> al.hasEvent(WAN_SYNC)));
    }

    private void clearAuditlogOnMembers(Cluster cluster) {
        for (HazelcastInstance hz : cluster.getMembers()) {
            TestAuditlogService auditlog = (TestAuditlogService) getAuditlogService(hz);
            auditlog.getEventQueue().clear();
        }
    }
}
