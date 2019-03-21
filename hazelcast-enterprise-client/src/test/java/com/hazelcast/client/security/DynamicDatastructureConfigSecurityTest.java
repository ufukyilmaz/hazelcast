package com.hazelcast.client.security;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.PermissionConfig;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.security.AccessControlException;

import static com.hazelcast.config.PermissionConfig.PermissionType.CONFIG;
import static com.hazelcast.test.HazelcastTestSupport.randomName;
import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class DynamicDatastructureConfigSecurityTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final TestHazelcastFactory factory = new TestHazelcastFactory();

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Test
    public void testDynamicDatastructureConfigAllowed_whenPermissionConfigured() {
        final Config config = createConfig();
        config.getSecurityConfig().addClientPermissionConfig(new PermissionConfig(CONFIG, null, "dev"));

        HazelcastInstance member = factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();

        String mapName = randomName();
        MapConfig mapConfig = new MapConfig(mapName).setBackupCount(4);
        client.getConfig().addMapConfig(mapConfig);
        assertEquals(mapConfig, member.getConfig().getMapConfig(mapName));
    }

    @Test
    public void testDynamicDatastructureConfigDenied_whenPermissionAbsent() {
        final Config config = createConfig();

        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();

        String mapName = randomName();
        MapConfig mapConfig = new MapConfig(mapName).setBackupCount(4);
        expectedException.expect(AccessControlException.class);
        client.getConfig().addMapConfig(mapConfig);
    }

    private Config createConfig() {
        final Config config = new Config();
        final SecurityConfig secCfg = config.getSecurityConfig();
        secCfg.setEnabled(true);
        return config;
    }
}
