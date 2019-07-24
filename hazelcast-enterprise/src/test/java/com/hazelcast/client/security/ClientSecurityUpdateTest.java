package com.hazelcast.client.security;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.PermissionConfig;
import com.hazelcast.config.PermissionConfig.PermissionType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.security.AccessControlException;
import java.util.Collections;

import static com.hazelcast.security.permission.ActionConstants.ACTION_ALL;
import static com.hazelcast.security.permission.ActionConstants.ACTION_PUT;
import static com.hazelcast.security.permission.ActionConstants.ACTION_READ;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientSecurityUpdateTest extends HazelcastTestSupport {

    private TestHazelcastFactory factory = new TestHazelcastFactory();

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Test
    public void testAddNewPermission() {
        Config config = new Config();
        config.getSecurityConfig().setEnabled(true);

        String name = "test";

        HazelcastInstance member = factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();

        // create map
        member.getMap(name).size();

        IMap<Object, Object> map = client.getMap(name);

        try {
            map.put("key", "value");
            fail("AccessControlException is expected!");
        } catch (AccessControlException expected) {
        }

        member.getConfig().getSecurityConfig()
                .setClientPermissionConfigs(singleton(new PermissionConfig(PermissionType.MAP, name, "*")
                        .addAction(ACTION_PUT)));

        // write allowed
        map.put("key", "value");
    }

    @Test
    public void testModifyExistingPermission() {
        Config config = new Config();
        config.getSecurityConfig().setEnabled(true);

        String name = "test";
        PermissionConfig readPermission = new PermissionConfig(PermissionType.MAP, name, "*")
                .addAction(ACTION_READ).addAction(ActionConstants.ACTION_CREATE);
        config.getSecurityConfig().setClientPermissionConfigs(singleton(readPermission));

        HazelcastInstance member = factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();

        // create & read allowed
        IMap<Object, Object> map = client.getMap(name);
        map.size();

        PermissionConfig readWritePermission = new PermissionConfig(readPermission).addAction(ACTION_PUT);
        member.getConfig().getSecurityConfig().setClientPermissionConfigs(singleton(readWritePermission));

        // write allowed
        map.put("key", "value");
        // read allowed
        assertEquals("value", map.get("key"));
    }

    @Test
    public void testRemovePermission() {
        Config config = new Config();
        config.getSecurityConfig().setEnabled(true);

        String name = "test";
        config.getSecurityConfig()
                .setClientPermissionConfigs(singleton(new PermissionConfig(PermissionType.MAP, name, "*")
                        .addAction(ACTION_ALL)));

        HazelcastInstance member = factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();

        IMap<Object, Object> map = client.getMap(name);

        member.getConfig().getSecurityConfig()
                .setClientPermissionConfigs(Collections.<PermissionConfig>emptySet());

        try {
            map.put("key", "value");
            fail("AccessControlException is expected!");
        } catch (AccessControlException expected) {
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testClientSide_permissionUpdate() {
        Config config = new Config();
        config.getSecurityConfig().setEnabled(true);

        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();

        client.getConfig().getSecurityConfig().setClientPermissionConfigs(Collections.<PermissionConfig>emptySet());

    }
}
