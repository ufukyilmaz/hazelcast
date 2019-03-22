package com.hazelcast.client.security;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.PermissionConfig;
import com.hazelcast.config.PermissionConfig.PermissionType;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.crdt.pncounter.PNCounter;
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

import java.security.AccessControlException;

import static com.hazelcast.test.HazelcastTestSupport.randomString;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class PnCounterSecurityTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final TestHazelcastFactory factory = new TestHazelcastFactory();
    private final String testObjectName = randomString();

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Test
    public void testPNCounterAllPermissions() {
        final Config config = createConfig();
        addPermission(config, testObjectName)
                .addAction(ActionConstants.ACTION_ALL);

        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();
        PNCounter counter = client.getPNCounter(testObjectName);
        counter.addAndGet(1);
        counter.get();
        counter.destroy();
    }

    @Test
    public void pnCounterCreateShouldFailWhenNoPermission() {
        final Config config = createConfig();
        addPermission(config, testObjectName)
                .addAction(ActionConstants.ACTION_READ);
        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();

        expectedException.expect(AccessControlException.class);
        client.getPNCounter(testObjectName);
    }

    @Test
    public void pnCounterGetShouldFailWhenNoPermission() {
        final Config config = createConfig();
        addPermission(config, testObjectName)
                .addAction(ActionConstants.ACTION_CREATE);
        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();

        PNCounter counter = client.getPNCounter(testObjectName);
        expectedException.expect(AccessControlException.class);
        counter.get();
    }

    @Test
    public void pnCounterAddShouldFailWhenNoPermission() {
        final Config config = createConfig();
        addPermission(config, testObjectName)
                .addAction(ActionConstants.ACTION_CREATE)
                .addAction(ActionConstants.ACTION_READ);
        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();

        PNCounter counter = client.getPNCounter(testObjectName);
        expectedException.expect(AccessControlException.class);
        counter.addAndGet(1);
    }

    private Config createConfig() {
        final Config config = new Config();
        final SecurityConfig secCfg = config.getSecurityConfig();
        secCfg.setEnabled(true);
        return config;
    }

    private PermissionConfig addPermission(Config config, String name) {
        PermissionConfig perm = new PermissionConfig(PermissionType.PN_COUNTER, name, "dev");
        config.getSecurityConfig().addClientPermissionConfig(perm);
        return perm;
    }

}