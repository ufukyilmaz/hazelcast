package com.hazelcast.security.auditlog;

import static com.hazelcast.auditlog.AuditlogTypeIds.CLUSTER_MEMBER_ADDED;
import static com.hazelcast.auditlog.AuditlogTypeIds.CLUSTER_MEMBER_REMOVED;
import static com.hazelcast.auditlog.AuditlogTypeIds.CLUSTER_MEMBER_SUSPECTED;
import static com.hazelcast.auditlog.AuditlogTypeIds.CLUSTER_MERGE;
import static com.hazelcast.auditlog.AuditlogTypeIds.CLUSTER_PROMOTE_MEMBER;
import static com.hazelcast.auditlog.AuditlogTypeIds.CLUSTER_SHUTDOWN;
import static com.hazelcast.test.Accessors.getAuditlogService;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.auditlog.AuditlogService;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({ QuickTest.class, ParallelJVMTest.class })
public class AuditLogTest extends HazelcastTestSupport {

    @Test
    public void testAuditlogDisabledByDefault() {
        Config config = smallInstanceConfig();
        config.getAuditlogConfig().setFactoryClassName(TestAuditlogServiceFactory.class.getName())
                .setProperty("property1", "value1").setProperty("property2", "value2");

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        assertThat(getAuditlogService(hz), is(not(instanceOf(TestAuditlogService.class))));
    }

    @Test
    public void testAuditlogEnabled() {
        Config config = smallInstanceConfig();
        config.getAuditlogConfig().setEnabled(true).setFactoryClassName(TestAuditlogServiceFactory.class.getName())
                .setProperty("property1", "value1").setProperty("property2", "value2");

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        AuditlogService auditlogService = getAuditlogService(hz);
        assertThat(auditlogService, is(instanceOf(TestAuditlogService.class)));

        TestAuditlogService auditlog = (TestAuditlogService) auditlogService;
        assertEquals("value1", auditlog.getProperties().getProperty("property1"));
        assertNotNull(auditlog.getCallbackHandler());

        HazelcastInstance hz2 = factory.newHazelcastInstance(smallInstanceConfig());
        auditlog.assertEventPresentEventually(CLUSTER_MEMBER_ADDED);
        auditlog.getEventQueue().clear();
        hz2.shutdown();
        auditlog.assertEventPresentEventually(CLUSTER_MEMBER_REMOVED);
        hz.shutdown();
    }

    @Test
    public void testPromoteLiteMember() {
        Config config = smallInstanceConfig();
        config.setLiteMember(true).getAuditlogConfig().setEnabled(true)
                .setFactoryClassName(TestAuditlogServiceFactory.class.getName());

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        factory.newHazelcastInstance(smallInstanceConfig());
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        TestAuditlogService auditlog = (TestAuditlogService) getAuditlogService(hz2);
        hz2.getCluster().promoteLocalLiteMember();
        auditlog.assertEventPresentEventually(CLUSTER_PROMOTE_MEMBER);
        auditlog.assertEventsNotPresent(CLUSTER_MEMBER_REMOVED, CLUSTER_MEMBER_SUSPECTED, CLUSTER_MERGE,
                CLUSTER_SHUTDOWN);
        auditlog.getEventQueue().clear();
    }
}
