package com.hazelcast.client.security;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.PermissionConfig;
import com.hazelcast.config.PermissionConfig.PermissionType;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class QueueSecurityTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final TestHazelcastFactory factory = new TestHazelcastFactory();
    private final String testObjectName = randomString();

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Test
    public void testQueuePermission() {
        final Config config = createConfig();
        addPermission(config, testObjectName)
                .addAction(ActionConstants.ACTION_ADD).addAction(ActionConstants.ACTION_CREATE);
        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();
        assertTrue(client.getQueue(testObjectName).offer("value"));
    }

    @Test
    public void testQueuePermissionFail() {
        final Config config = createConfig();
        addPermission(config, testObjectName);
        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();
        expectedException.expect(RuntimeException.class);
        client.getQueue(testObjectName).offer("value");
    }

    private Config createConfig() {
        final Config config = new Config();
        final SecurityConfig secCfg = config.getSecurityConfig();
        secCfg.setEnabled(true);
        return config;
    }

    private PermissionConfig addPermission(Config config, String name) {
        PermissionConfig perm = new PermissionConfig(PermissionType.QUEUE, name, "dev");
        config.getSecurityConfig().addClientPermissionConfig(perm);
        return perm;
    }
}
