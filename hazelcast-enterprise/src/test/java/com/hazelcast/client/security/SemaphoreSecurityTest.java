package com.hazelcast.client.security;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.PermissionConfig;
import com.hazelcast.config.PermissionConfig.PermissionType;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.config.cp.SemaphoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.ISemaphore;
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
import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SemaphoreSecurityTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final TestHazelcastFactory factory = new TestHazelcastFactory();
    private final String testObjectName = randomString();

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Test
    public void testSemaphoreReadPermission() {
        final Config config = createConfig();
        config.getCPSubsystemConfig().setCPMemberCount(3);
        addPermission(config)
                .addAction(ActionConstants.ACTION_CREATE)
                .addAction(ActionConstants.ACTION_READ);

        HazelcastInstance client = newClient(config);
        ISemaphore semaphore = client.getCPSubsystem().getSemaphore(testObjectName);
        assertEquals(0, semaphore.availablePermits());
    }

    @Test
    public void testSemaphoreAcquirePermission() {
        final Config config = createConfig();
        config.getCPSubsystemConfig().setCPMemberCount(3);
        addPermission(config)
                .addAction(ActionConstants.ACTION_CREATE)
                .addAction(ActionConstants.ACTION_ACQUIRE);

        HazelcastInstance client = newClient(config);
        ISemaphore semaphore = client.getCPSubsystem().getSemaphore(testObjectName);
        semaphore.tryAcquire();
    }

    @Test
    public void testSemaphoreReleasePermission() {
        final Config config = createConfig();
        config.getCPSubsystemConfig().setCPMemberCount(3);
        addPermission(config)
                .addAction(ActionConstants.ACTION_CREATE)
                .addAction(ActionConstants.ACTION_RELEASE);

        HazelcastInstance client = newClient(config);
        ISemaphore semaphore = client.getCPSubsystem().getSemaphore(testObjectName);
        semaphore.release();
    }

    @Test
    public void testSemaphore_acquireFail_withoutPermission() {
        final Config config = createConfig();
        config.getCPSubsystemConfig().setCPMemberCount(3);
        addPermission(config)
                .addAction(ActionConstants.ACTION_CREATE);

        HazelcastInstance client = newClient(config);
        ISemaphore semaphore = client.getCPSubsystem().getSemaphore(testObjectName);
        expectedException.expect(AccessControlException.class);
        semaphore.tryAcquire();
    }

    @Test
    public void testSemaphore_releaseFail_withoutPermission() {
        final Config config = createConfig();
        config.getCPSubsystemConfig().setCPMemberCount(3);
        addPermission(config)
                .addAction(ActionConstants.ACTION_CREATE);

        HazelcastInstance client = newClient(config);
        ISemaphore semaphore = client.getCPSubsystem().getSemaphore(testObjectName);
        expectedException.expect(AccessControlException.class);
        semaphore.release();
    }

    @Test
    public void testSemaphore_readFail_withoutPermission() {
        final Config config = createConfig();
        config.getCPSubsystemConfig().setCPMemberCount(3);
        addPermission(config)
                .addAction(ActionConstants.ACTION_CREATE);

        HazelcastInstance client = newClient(config);
        ISemaphore semaphore = client.getCPSubsystem().getSemaphore(testObjectName);
        expectedException.expect(AccessControlException.class);
        semaphore.availablePermits();
    }

    private Config createConfig() {
        Config config = new Config();
        SecurityConfig secCfg = config.getSecurityConfig();
        secCfg.setEnabled(true);

        config.getCPSubsystemConfig().addSemaphoreConfig(new SemaphoreConfig(testObjectName).setJDKCompatible(true));
        return config;
    }

    private PermissionConfig addPermission(Config config) {
        PermissionConfig perm = new PermissionConfig(PermissionType.SEMAPHORE, testObjectName, "dev");
        config.getSecurityConfig().addClientPermissionConfig(perm);
        return perm;
    }

    private HazelcastInstance newClient(Config config) {
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        return factory.newHazelcastClient();
    }
}
