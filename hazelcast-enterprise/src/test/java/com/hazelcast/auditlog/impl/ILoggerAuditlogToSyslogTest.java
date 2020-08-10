package com.hazelcast.auditlog.impl;

import static com.hazelcast.auditlog.AuditlogTypeIds.AUTHENTICATION_MEMBER;
import static com.hazelcast.auditlog.AuditlogTypeIds.CLUSTER_SHUTDOWN;
import static com.hazelcast.auditlog.AuditlogTypeIds.NETWORK_CONNECT;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.config.Config;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.ChangeLoggingRule;
import com.hazelcast.test.TestAwareInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.auditlog.SyslogServerRule;

/**
 * Tests ILogger auditlog with syslog server used as the message sink. See {@code resources/log4j2-syslog.xml} for Syslog
 * appender configuration.
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({ QuickTest.class })
public class ILoggerAuditlogToSyslogTest {

    @ClassRule
    public static SyslogServerRule syslogRule = SyslogServerRule.create(4514);

    @ClassRule
    public static ChangeLoggingRule changeLoggingRule = new ChangeLoggingRule("log4j2-syslog.xml");

    private final TestAwareInstanceFactory factory = new TestAwareInstanceFactory();

    @After
    public void after() {
        factory.terminateAll();
        syslogRule.getEventQueue().clear();
    }

    @Test
    public void testAuditLogEnabled() throws IOException {
        Config config = smallInstanceConfig();
        config.getAuditlogConfig().setEnabled(true);
        config.getSecurityConfig().setEnabled(true);
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config).getCluster().shutdown();
        syslogRule.assertSyslogContainsEventually(NETWORK_CONNECT, AUTHENTICATION_MEMBER, CLUSTER_SHUTDOWN);
    }

    @Test
    public void testAuditLogDisabledByDefault() throws IOException {
        Config config = smallInstanceConfig();
        config.getSecurityConfig().setEnabled(true);
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config).getCluster().shutdown();
        assertTrue(syslogRule.getEventQueue().isEmpty());
    }
}
