package com.hazelcast.internal.auditlog.impl;

import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.config.Config;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.internal.auditlog.impl.AuditlogTypeIds;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.TestAwareInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;

/**
 * Tests ILogger auditlog implementation. See {@code resources/log4j2.xml} for AuditLotFile appender configuration.
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({ QuickTest.class })
public class ILoggerAuditlogServiceTest {

    private final TestAwareInstanceFactory factory = new TestAwareInstanceFactory();
    private final File file = new File(System.getProperty("java.io.tmpdir"), "hazelcast-auditlog.log");

    @Before
    public void before() {
        if (file.exists()) {
            // reset auditlog contents
            try {
                FileChannel.open(file.toPath(), StandardOpenOption.WRITE).truncate(0).close();
            } catch (IOException e) {
                fail();
            }
        }
    }

    @Test
    public void testAuditLogEnabled() throws IOException {
        testAuditLogInternal(smallInstanceConfig().setProperty(GroupProperty.AUDIT_LOG_ENABLED.getName(), "true"), true);
    }

    @Test
    public void testAuditLogDisabled() throws IOException {
        testAuditLogInternal(smallInstanceConfig().setProperty(GroupProperty.AUDIT_LOG_ENABLED.getName(), "false"), false);
    }

    @Test
    public void testAuditLogDisabledByDefault() throws IOException {
        testAuditLogInternal(smallInstanceConfig(), false);
    }

    private void testAuditLogInternal(Config config, boolean logExpected) throws IOException {
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        factory.terminateAll();
        if (!file.exists()) {
            assertFalse("Auditlog file doesn't exist", logExpected);
            return;
        }
        Optional<String> line = Files.lines(file.toPath()).filter(l -> l.contains(AuditlogTypeIds.CONNECTION_ASKS_PROTOCOL))
                .findFirst();
        assertEquals("Auditlog entry presence", logExpected, line.isPresent());
    }
}
