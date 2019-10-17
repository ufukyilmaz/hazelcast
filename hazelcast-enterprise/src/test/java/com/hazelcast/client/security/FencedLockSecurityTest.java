package com.hazelcast.client.security;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.PermissionConfig;
import com.hazelcast.config.PermissionConfig.PermissionType;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.lock.FencedLock;
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

import java.security.AccessControlException;

import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class FencedLockSecurityTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final TestHazelcastFactory factory = new TestHazelcastFactory();
    private final String testObjectName = randomString();

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Test
    public void testFencedLockPermission() {
        final Config config = createConfig();
        addPermission(config)
                .addAction(ActionConstants.ACTION_CREATE).addAction(ActionConstants.ACTION_LOCK);
        HazelcastInstance client = newClient(config);
        FencedLock lock = client.getCPSubsystem().getLock(testObjectName);
        assertTrue(lock.tryLock());
        lock.unlock();
    }

    @Test
    public void testFencedLock_lockFail_withoutPermission() {
        final Config config = createConfig();
        addPermission(config)
                .addAction(ActionConstants.ACTION_CREATE);
        HazelcastInstance client = newClient(config);
        FencedLock lock = client.getCPSubsystem().getLock(testObjectName);
        expectedException.expect(AccessControlException.class);
        lock.tryLock();
    }

    private Config createConfig() {
        Config config = new Config();
        SecurityConfig secCfg = config.getSecurityConfig();
        secCfg.setEnabled(true);
        return config;
    }

    private PermissionConfig addPermission(Config config) {
        PermissionConfig perm = new PermissionConfig(PermissionType.LOCK, testObjectName, "dev");
        config.getSecurityConfig().addClientPermissionConfig(perm);
        return perm;
    }

    private HazelcastInstance newClient(Config config) {
        factory.newHazelcastInstance(config);
        return factory.newHazelcastClient();
    }
}
