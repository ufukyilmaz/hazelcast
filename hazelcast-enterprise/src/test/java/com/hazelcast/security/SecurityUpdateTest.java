package com.hazelcast.security;

import com.hazelcast.config.Config;
import com.hazelcast.config.PermissionConfig;
import com.hazelcast.config.PermissionConfig.PermissionType;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.Set;

import static com.hazelcast.security.permission.ActionConstants.ACTION_ALL;
import static com.hazelcast.security.permission.ActionConstants.ACTION_READ;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SecurityUpdateTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void testEnable_atRuntime() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        HazelcastInstance hz = factory.newHazelcastInstance();
        SecurityConfig securityConfig = hz.getConfig().getSecurityConfig();

        exception.expect(UnsupportedOperationException.class);
        securityConfig.setEnabled(true);
    }

    @Test
    public void testUpdate_whenSecurityNotEnabled() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        HazelcastInstance hz = factory.newHazelcastInstance();
        SecurityConfig securityConfig = hz.getConfig().getSecurityConfig();

        exception.expect(UnsupportedOperationException.class);
        securityConfig.setClientPermissionConfigs(Collections.<PermissionConfig>emptySet());
    }

    @Test
    public void testAddNewPermission() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        Config config = new Config();
        config.getSecurityConfig().setEnabled(true);

        String name = "test";

        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);

        PermissionConfig permission = new PermissionConfig(PermissionType.MAP, name, "*").addAction(ACTION_ALL);
        hz1.getConfig().getSecurityConfig().setClientPermissionConfigs(singleton(permission));

        Set<PermissionConfig> permissionConfigs = hz2.getConfig().getSecurityConfig().getClientPermissionConfigs();
        assertEquals(1, permissionConfigs.size());
        assertContains(permissionConfigs, permission);
    }

    @Test
    public void testModifyExistingPermission() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        Config config = new Config();
        config.getSecurityConfig().setEnabled(true);

        String name = "test";

        PermissionConfig readPermission = new PermissionConfig(PermissionType.MAP, name, "*").addAction(ACTION_READ);
        config.getSecurityConfig().setClientPermissionConfigs(singleton(readPermission));

        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);

        PermissionConfig allPermission = new PermissionConfig(readPermission).addAction(ACTION_ALL);
        hz1.getConfig().getSecurityConfig().setClientPermissionConfigs(singleton(allPermission));

        Set<PermissionConfig> permissionConfigs = hz2.getConfig().getSecurityConfig().getClientPermissionConfigs();
        assertEquals(1, permissionConfigs.size());
        assertContains(permissionConfigs, allPermission);
    }

    @Test
    public void testRemovePermission() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        Config config = new Config();
        config.getSecurityConfig().setEnabled(true);

        String name = "test";

        PermissionConfig readPermission = new PermissionConfig(PermissionType.MAP, name, "*").addAction(ACTION_READ);
        config.getSecurityConfig().setClientPermissionConfigs(singleton(readPermission));

        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);

        hz1.getConfig().getSecurityConfig().setClientPermissionConfigs(Collections.<PermissionConfig>emptySet());

        Set<PermissionConfig> permissionConfigs = hz2.getConfig().getSecurityConfig().getClientPermissionConfigs();
        assertEquals(0, permissionConfigs.size());
    }

    @Test
    public void testReplication_onMemberJoin() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        Config config = new Config();
        config.getSecurityConfig().setEnabled(true);

        HazelcastInstance hz1 = factory.newHazelcastInstance(config);

        String name = "test";
        PermissionConfig permission = new PermissionConfig(PermissionType.MAP, name, "*").addAction(ACTION_ALL);
        hz1.getConfig().getSecurityConfig().setClientPermissionConfigs(singleton(permission));

        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        Set<PermissionConfig> permissionConfigs = hz2.getConfig().getSecurityConfig().getClientPermissionConfigs();
        assertEquals(1, permissionConfigs.size());
        assertContains(permissionConfigs, permission);
    }

    @Test
    public void testReplication_onLiteMemberJoin() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        Config config = new Config();
        config.getSecurityConfig().setEnabled(true);

        HazelcastInstance hz1 = factory.newHazelcastInstance(config);

        String name = "test";
        PermissionConfig permission = new PermissionConfig(PermissionType.MAP, name, "*").addAction(ACTION_ALL);
        hz1.getConfig().getSecurityConfig().setClientPermissionConfigs(singleton(permission));

        Config liteConfig = new Config().setLiteMember(true);
        liteConfig.getSecurityConfig().setEnabled(true);

        HazelcastInstance hz2 = factory.newHazelcastInstance(liteConfig);
        Set<PermissionConfig> permissionConfigs = hz2.getConfig().getSecurityConfig().getClientPermissionConfigs();
        assertEquals(1, permissionConfigs.size());
        assertContains(permissionConfigs, permission);
    }

}
