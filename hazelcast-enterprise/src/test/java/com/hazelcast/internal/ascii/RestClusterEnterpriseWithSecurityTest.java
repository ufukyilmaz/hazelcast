package com.hazelcast.internal.ascii;

import static com.hazelcast.security.loginmodules.TestLoginModule.PROPERTY_PRINCIPALS_SIMPLE;
import static com.hazelcast.security.loginmodules.TestLoginModule.PROPERTY_RESULT_COMMIT;
import static com.hazelcast.security.loginmodules.TestLoginModule.VALUE_ACTION_FAIL;
import static com.hazelcast.test.HazelcastTestSupport.assertClusterStateEventually;
import static org.junit.Assert.assertEquals;

import java.net.HttpURLConnection;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.config.LoginModuleConfig;
import com.hazelcast.config.LoginModuleConfig.LoginModuleUsage;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.security.loginmodules.TestLoginModule;
import com.hazelcast.test.annotation.QuickTest;

/**
 * Tests REST API calls with security enabled on Hazelcast members.
 */
@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category(QuickTest.class)
public class RestClusterEnterpriseWithSecurityTest extends RestClusterTest {

    private static final String WRONG_PASSWORD = "foo";

    @Override
    protected Config createConfigWithRestEnabled() {
        Config config = super.createConfigWithRestEnabled();
        config.getSecurityConfig().setEnabled(true);
        return config;
    }

    @Override
    protected String getPassword() {
        return "dev-pass";
    }

    @Test
    public void testWrongPasswordWithSecurity() throws Exception {
        Config config = createConfigWithRestEnabled();
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        String groupName = config.getGroupConfig().getName();
        assertEquals(STATUS_FORBIDDEN, communicator.shutdownMember(groupName, WRONG_PASSWORD));
        assertEquals(STATUS_FORBIDDEN, communicator.changeClusterState(groupName, WRONG_PASSWORD, "frozen").response);
        assertEquals(STATUS_FORBIDDEN, communicator.changeClusterVersion(groupName, WRONG_PASSWORD,
                instance.getCluster().getClusterVersion().toString()).response);
        assertEquals(STATUS_FORBIDDEN, communicator.hotBackup(groupName, WRONG_PASSWORD).response);
        assertEquals(STATUS_FORBIDDEN, communicator.hotBackupInterrupt(groupName, WRONG_PASSWORD).response);
        assertEquals(STATUS_FORBIDDEN, communicator.forceStart(groupName, WRONG_PASSWORD).response);
        assertEquals(STATUS_FORBIDDEN, communicator.partialStart(groupName, WRONG_PASSWORD).response);
        assertEquals(403,
                communicator.changeManagementCenterUrl(groupName, WRONG_PASSWORD, "http://bla").responseCode);
        assertEquals(STATUS_FORBIDDEN, communicator.listClusterNodes(groupName, WRONG_PASSWORD));
        assertEquals(STATUS_FORBIDDEN, communicator.shutdownCluster(groupName, WRONG_PASSWORD).response);
        assertEquals(STATUS_FORBIDDEN, communicator.getClusterState(groupName, WRONG_PASSWORD));
    }

    @Test
    public void testRestApiCallPassesWithCustomLoginModule() throws Exception {
        Config config = createConfigWithRestEnabled();
        Properties properties = new Properties();
        properties.setProperty(PROPERTY_PRINCIPALS_SIMPLE, "test");
        addCustomLoginModule(config.getSecurityConfig(), properties);
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        assertEquals(HttpURLConnection.HTTP_OK, communicator.changeClusterState("foo", "", "frozen").responseCode);
        assertClusterStateEventually(ClusterState.FROZEN, instance);
    }

    @Test
    public void testRestApiCallFailsWithCustomLoginModule() throws Exception {
        Config config = createConfigWithRestEnabled();
        Properties properties = new Properties();
        properties.setProperty(PROPERTY_PRINCIPALS_SIMPLE, "test");
        properties.setProperty(PROPERTY_RESULT_COMMIT, VALUE_ACTION_FAIL);
        addCustomLoginModule(config.getSecurityConfig(), properties);
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        assertEquals(STATUS_FORBIDDEN, communicator.changeClusterState("foo", "", "frozen").response);
    }

    private void addCustomLoginModule(SecurityConfig securityConfig, Properties properties) {
        LoginModuleConfig loginModuleConfig = new LoginModuleConfig();
        loginModuleConfig.setClassName(TestLoginModule.class.getName());
        loginModuleConfig.setUsage(LoginModuleUsage.REQUIRED);
        loginModuleConfig.setProperties(properties);
        securityConfig.addMemberLoginModuleConfig(loginModuleConfig);
    }
}
