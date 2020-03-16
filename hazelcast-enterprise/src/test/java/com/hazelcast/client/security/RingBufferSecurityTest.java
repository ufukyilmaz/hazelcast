package com.hazelcast.client.security;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.PermissionConfig;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static com.hazelcast.test.HazelcastTestSupport.randomString;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RingBufferSecurityTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final TestHazelcastFactory factory = new TestHazelcastFactory();
    private final String testObjectName = randomString();

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Test
    public void testRingBufferPermission() {
        final Config config = createConfig();
        addPermission(config, testObjectName)
                .addAction(ActionConstants.ACTION_PUT).addAction(ActionConstants.ACTION_CREATE);
        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();
        client.getRingbuffer(testObjectName).add("value");
    }

    @Test
    public void testRingBufferPermissionFail() {
        final Config config = createConfig();
        addPermission(config, testObjectName)
                .addAction(ActionConstants.ACTION_CREATE);
        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();
        expectedException.expect(RuntimeException.class);
        client.getRingbuffer(testObjectName).add("value");
    }

    private Config createConfig() {
        final Config config = new Config();
        final SecurityConfig secCfg = config.getSecurityConfig();
        secCfg.setEnabled(true);
        return config;
    }

    private PermissionConfig addPermission(Config config, String name) {
        PermissionConfig perm = new PermissionConfig(PermissionConfig.PermissionType.RING_BUFFER, name, "dev");
        config.getSecurityConfig().addClientPermissionConfig(perm);
        return perm;
    }
}
