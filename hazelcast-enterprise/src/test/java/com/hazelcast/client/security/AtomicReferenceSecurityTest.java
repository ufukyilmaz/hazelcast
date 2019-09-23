package com.hazelcast.client.security;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.PermissionConfig;
import com.hazelcast.config.PermissionConfig.PermissionType;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.IAtomicReference;
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
import static org.junit.Assert.assertNull;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AtomicReferenceSecurityTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final TestHazelcastFactory factory = new TestHazelcastFactory();
    private final String testObjectName = randomString();

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Test
    public void testAtomicReferenceReadPermission() {
        final Config config = createConfig();
        config.getCPSubsystemConfig().setCPMemberCount(3);
        addPermission(config)
                .addAction(ActionConstants.ACTION_CREATE)
                .addAction(ActionConstants.ACTION_READ);

        HazelcastInstance client = newClient(config);
        IAtomicReference atomic = client.getCPSubsystem().getAtomicReference(testObjectName);
        assertNull(atomic.get());
    }

    @Test
    public void testAtomicReferenceWritePermission() {
        final Config config = createConfig();
        config.getCPSubsystemConfig().setCPMemberCount(3);
        addPermission(config)
                .addAction(ActionConstants.ACTION_CREATE)
                .addAction(ActionConstants.ACTION_MODIFY);

        HazelcastInstance client = newClient(config);
        IAtomicReference atomic = client.getCPSubsystem().getAtomicReference(testObjectName);
        assertNull(atomic.getAndSet("value"));
    }

    @Test
    public void testAtomicReference_modifyFail_withoutPermission() {
        final Config config = createConfig();
        config.getCPSubsystemConfig().setCPMemberCount(3);
        addPermission(config)
                .addAction(ActionConstants.ACTION_CREATE);

        HazelcastInstance client = newClient(config);
        IAtomicReference atomic = client.getCPSubsystem().getAtomicReference(testObjectName);
        expectedException.expect(AccessControlException.class);
        atomic.set("value");
    }

    @Test
    public void testAtomicReference_readFail_withoutPermission() {
        final Config config = createConfig();
        config.getCPSubsystemConfig().setCPMemberCount(3);
        addPermission(config)
                .addAction(ActionConstants.ACTION_CREATE);

        HazelcastInstance client = newClient(config);
        IAtomicReference atomic = client.getCPSubsystem().getAtomicReference(testObjectName);
        expectedException.expect(AccessControlException.class);
        atomic.get();
    }

    private Config createConfig() {
        Config config = new Config();
        SecurityConfig secCfg = config.getSecurityConfig();
        secCfg.setEnabled(true);
        return config;
    }

    private PermissionConfig addPermission(Config config) {
        PermissionConfig perm = new PermissionConfig(PermissionType.ATOMIC_REFERENCE, testObjectName, "dev");
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
