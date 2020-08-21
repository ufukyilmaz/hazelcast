package com.hazelcast.client.security;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.PermissionConfig;
import com.hazelcast.config.PermissionConfig.PermissionType;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.map.IMap;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.security.AccessControlException;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Test for SQL security permission.
 */
@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlSecurityTest {

    private static final String MAP_NAME = "map";

    private final TestHazelcastFactory factory = new TestHazelcastFactory();

    @After
    public void after() {
        factory.terminateAll();
    }

    @Test
    public void testSuccess() {
        checkPermission(true);
    }

    @Test
    public void testFailure() {
        checkPermission(false);
    }

    private void checkPermission(boolean allowed) {
        Config config = createConfig();

        config.getSecurityConfig().addClientPermissionConfig(
            new PermissionConfig(PermissionType.MAP, MAP_NAME, null).addAction(ActionConstants.ACTION_ALL)
        );

        if (allowed) {
            config.getSecurityConfig().addClientPermissionConfig(
                    new PermissionConfig(PermissionType.SQL, null, null).setEndpoints(Collections.singleton("127.0.0.1"))
            );
        }

        HazelcastInstance member = factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();

        IMap<Integer, Integer> map = member.getMap(MAP_NAME);
        map.put(1, 1);

        if (allowed) {
            client.getSql().execute("SELECT * FROM " + MAP_NAME).close();
            client.getSql().execute(new SqlStatement("SELECT * FROM " + MAP_NAME)).close();
        } else {
            try {
                client.getSql().execute("SELECT * FROM " + MAP_NAME).close();

                fail("Must fail");
            } catch (Exception e) {
                assertEquals(AccessControlException.class, e.getClass());
                assertEquals(
                    "Permission (\"com.hazelcast.security.permission.SqlPermission\" \"<sql>\" \"sql\") denied!",
                    e.getMessage()
                );
            }

            try {
                client.getSql().execute(new SqlStatement("SELECT * FROM " + MAP_NAME)).close();

                fail("Must fail");
            } catch (Exception e) {
                assertEquals(AccessControlException.class, e.getClass());
                assertEquals(
                    "Permission (\"com.hazelcast.security.permission.SqlPermission\" \"<sql>\" \"sql\") denied!",
                    e.getMessage()
                );
            }
        }
    }

    private Config createConfig() {
        final Config config = new Config();
        final SecurityConfig secCfg = config.getSecurityConfig();
        secCfg.setEnabled(true);
        return config;
    }
}
