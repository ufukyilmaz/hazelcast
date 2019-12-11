package com.hazelcast.internal.ascii;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.config.LoginModuleConfig;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.config.security.JaasAuthenticationConfig;
import com.hazelcast.config.security.RealmConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.security.loginmodules.TestLoginModule;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.HttpURLConnection;
import java.util.Properties;

import static com.hazelcast.config.LoginModuleConfig.LoginModuleUsage.REQUIRED;
import static com.hazelcast.security.loginmodules.TestLoginModule.PROPERTY_PRINCIPALS_SIMPLE;
import static com.hazelcast.security.loginmodules.TestLoginModule.PROPERTY_RESULT_COMMIT;
import static com.hazelcast.security.loginmodules.TestLoginModule.VALUE_ACTION_FAIL;
import static com.hazelcast.test.AbstractHazelcastClassRunner.getTestMethodName;
import static com.hazelcast.test.HazelcastTestSupport.assertClusterStateEventually;
import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static org.junit.Assert.assertEquals;

/**
 * Tests REST API calls with security enabled on Hazelcast members.
 */
@Category(QuickTest.class)
public class RestClusterEnterpriseWithSecurityTest extends AbstractRestClusterEnterpriseTest {

    private static final String WRONG_PASSWORD = "foo";

    @Override
    protected Config createConfigWithRestEnabled() {
        Config config = super.createConfigWithRestEnabled();
        config.getSecurityConfig().setEnabled(true).setMemberRealmConfig("realm",
                new RealmConfig().setUsernamePasswordIdentityConfig(getTestMethodName(), "dev-pass"));
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
        String clusterName = config.getClusterName();
        assertEquals(HTTP_FORBIDDEN,
                communicator.shutdownMember(clusterName, WRONG_PASSWORD).responseCode);
        assertEquals(HTTP_FORBIDDEN,
                communicator.changeClusterState(clusterName, WRONG_PASSWORD, "frozen").responseCode);
        assertEquals(HTTP_FORBIDDEN,
                communicator.changeClusterVersion(clusterName, WRONG_PASSWORD,
                        instance.getCluster().getClusterVersion().toString()).responseCode);
        assertEquals(HTTP_FORBIDDEN,
                communicator.hotBackup(clusterName, WRONG_PASSWORD).responseCode);
        assertEquals(HTTP_FORBIDDEN,
                communicator.hotBackupInterrupt(clusterName, WRONG_PASSWORD).responseCode);
        assertEquals(HTTP_FORBIDDEN,
                communicator.forceStart(clusterName, WRONG_PASSWORD).responseCode);
        assertEquals(HTTP_FORBIDDEN, communicator.partialStart(clusterName, WRONG_PASSWORD).responseCode);
        assertEquals(HTTP_FORBIDDEN, communicator.listClusterNodes(clusterName, WRONG_PASSWORD).responseCode);
        assertEquals(HTTP_FORBIDDEN, communicator.shutdownCluster(clusterName, WRONG_PASSWORD).responseCode);
        assertEquals(HTTP_FORBIDDEN, communicator.getClusterState(clusterName, WRONG_PASSWORD).responseCode);
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
        assertEquals(HTTP_FORBIDDEN, communicator.changeClusterState("foo", "", "frozen").responseCode);
    }

    private void addCustomLoginModule(SecurityConfig securityConfig, Properties properties) {
        LoginModuleConfig loginModuleConfig = new LoginModuleConfig(TestLoginModule.class.getName(), REQUIRED)
                .setProperties(properties);
        RealmConfig realmConfig = new RealmConfig()
                .setJaasAuthenticationConfig(new JaasAuthenticationConfig().addLoginModuleConfig(loginModuleConfig));
        securityConfig.setMemberRealmConfig("realm", realmConfig);
    }
}
