package com.hazelcast.security.auditlog;

import static com.hazelcast.auditlog.AuditlogTypeIds.CLUSTER_MEMBER_SUSPECTED;
import static com.hazelcast.auditlog.AuditlogTypeIds.CLUSTER_MERGE;
import static com.hazelcast.test.Accessors.getAuditlogService;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;

/**
 * Auditlog test for split brain related events.
 */
@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({ QuickTest.class, ParallelJVMTest.class })
public class AuditLogSplitBrainTest extends SplitBrainTestSupport {

    @Override
    protected Config config() {
        Config config = super.config();
        config.getAuditlogConfig().setEnabled(true).setFactoryClassName(TestAuditlogServiceFactory.class.getName());
        return config;
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) throws Exception {
        boolean suspectedEventPresent = false;
        for (HazelcastInstance hz : firstBrain) {
            TestAuditlogService auditlog = (TestAuditlogService) getAuditlogService(hz);
            suspectedEventPresent |= auditlog.hasEvent(CLUSTER_MEMBER_SUSPECTED);
            auditlog.assertEventsNotPresent(CLUSTER_MERGE);
            auditlog.getEventQueue().clear();
        }
        assertTrue("Member suspected event should be present in the first brain", suspectedEventPresent);

        suspectedEventPresent = false;
        for (HazelcastInstance hz : secondBrain) {
            TestAuditlogService auditlog = (TestAuditlogService) getAuditlogService(hz);
            suspectedEventPresent |= auditlog.hasEvent(CLUSTER_MEMBER_SUSPECTED);
            auditlog.assertEventsNotPresent(CLUSTER_MERGE);
            auditlog.getEventQueue().clear();
        }
        assertTrue("Member suspected event should be present in the second brain", suspectedEventPresent);
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances) throws Exception {
        for (HazelcastInstance hz : instances) {
            TestAuditlogService auditlog = (TestAuditlogService) getAuditlogService(hz);
            if (auditlog.hasEvent(CLUSTER_MERGE)) {
                return;
            }
        }
        fail("The CLUSTER_MERGE event is not present");
    }

}
