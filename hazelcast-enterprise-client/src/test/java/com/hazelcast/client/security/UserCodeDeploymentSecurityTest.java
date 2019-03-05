package com.hazelcast.client.security;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientUserCodeDeploymentConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.PermissionConfig;
import com.hazelcast.config.PermissionConfig.PermissionType;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.FilteringClassLoader;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import usercodedeployment.IncrementingEntryProcessor;

import static com.hazelcast.config.PermissionConfig.PermissionType.MAP;
import static com.hazelcast.config.PermissionConfig.PermissionType.USER_CODE_DEPLOYMENT;
import static com.hazelcast.test.HazelcastTestSupport.randomName;
import static java.util.Collections.singletonList;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class UserCodeDeploymentSecurityTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final TestHazelcastFactory factory = new TestHazelcastFactory();

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Test(expected = IllegalStateException.class)
    public void testUserCodeDeploymentNotAllowed() {
        testUserCodeDeployment(ActionConstants.ACTION_ADD);
    }

    @Test
    public void testUserCodeDeploymentDeployAllowed() {
        testUserCodeDeployment(ActionConstants.ACTION_USER_CODE_DEPLOY);
    }

    @Test
    public void testUserCodeDeploymentDeployAllAllowed() {
        testUserCodeDeployment(ActionConstants.ACTION_ALL);
    }

    private void testUserCodeDeployment(String actionType) {
        Config config = createConfig();
        addPermission(config, USER_CODE_DEPLOYMENT, null)
                .addAction(actionType);
        addPermission(config, MAP, "*")
                .addAction(ActionConstants.ACTION_ALL);
        FilteringClassLoader filteringCL = new FilteringClassLoader(singletonList("usercodedeployment"), null);
        config.setClassLoader(filteringCL);
        config.getUserCodeDeploymentConfig()
              .setEnabled(true);
        factory.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        ClientUserCodeDeploymentConfig clientUserCodeDeploymentConfig = new ClientUserCodeDeploymentConfig();
        clientUserCodeDeploymentConfig.addClass("usercodedeployment.IncrementingEntryProcessor");
        clientConfig.setUserCodeDeploymentConfig(clientUserCodeDeploymentConfig.setEnabled(true));
        HazelcastInstance client = factory.newHazelcastClient(clientConfig);

        IncrementingEntryProcessor incrementingEntryProcessor = new IncrementingEntryProcessor();
        IMap<Integer, Integer> map = client.getMap(randomName());
        map.put(1, 1);
        map.executeOnEntries(incrementingEntryProcessor);
    }

    private Config createConfig() {
        final Config config = new Config();
        final SecurityConfig secCfg = config.getSecurityConfig();
        secCfg.setEnabled(true);
        return config;
    }

    private PermissionConfig addPermission(Config config, PermissionType type, String name) {
        PermissionConfig perm = new PermissionConfig(type, name, "dev");
        config.getSecurityConfig().addClientPermissionConfig(perm);
        return perm;
    }
}
