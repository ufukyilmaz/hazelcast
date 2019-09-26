package com.hazelcast.security;

import com.hazelcast.config.Config;
import com.hazelcast.config.CredentialsFactoryConfig;
import com.hazelcast.config.LoginModuleConfig;
import com.hazelcast.config.LoginModuleConfig.LoginModuleUsage;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.security.impl.DefaultLoginModule;
import com.hazelcast.security.loginmodules.TestLoginModule;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Properties;

import static com.hazelcast.security.loginmodules.TestLoginModule.PROPERTY_PRINCIPALS_SIMPLE;
import static com.hazelcast.security.loginmodules.TestLoginModule.PROPERTY_RESULT_COMMIT;
import static com.hazelcast.security.loginmodules.TestLoginModule.PROPERTY_RESULT_LOGIN;
import static com.hazelcast.security.loginmodules.TestLoginModule.VALUE_ACTION_FAIL;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({ QuickTest.class, ParallelJVMTest.class })
public class MemberSecurityTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Test
    public void testAcceptMember() {
        final Config config = new Config();
        final SecurityConfig secCfg = config.getSecurityConfig();
        secCfg.setEnabled(true);

        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        factory.newHazelcastInstance(config); // master
        HazelcastInstance member = factory.newHazelcastInstance(config);
        assertClusterSize(2, member);
    }

    @Test
    public void testDenyMemberWrongCredentials() {
        final Config config = new Config();
        final SecurityConfig secCfg = config.getSecurityConfig();
        secCfg.setEnabled(true);
        CredentialsFactoryConfig credentialsFactoryConfig = new CredentialsFactoryConfig();
        credentialsFactoryConfig.setImplementation(new ICredentialsFactory() {
            @Override
            public Credentials newCredentials() {
                return new UsernamePasswordCredentials("invalid", "credentials");
            }

            @Override
            public void destroy() {
            }

            @Override
            public void configure(String clusterName, String clusterPassword, Properties properties) {
            }
        });
        secCfg.setMemberCredentialsConfig(credentialsFactoryConfig);

        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        factory.newHazelcastInstance(config); // master
        expected.expect(IllegalStateException.class);
        factory.newHazelcastInstance(config);
    }

    /**
     * <pre>
     * Given: security with a custom member loginmodule is enabled in configuration
     * When: member during successful login gets 2 JAAS Principals (not ClusterPrincipal instances)
     * Then: members successfully join and form a cluster
     * </pre>
     */
    @Test
    public void testNoClusterPrincipal() {
        Properties properties = new Properties();
        properties.setProperty(PROPERTY_PRINCIPALS_SIMPLE, "test1,test2");
        final Config config = createTestLoginModuleConfig(properties);
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        factory.newHazelcastInstance(config); // master
        HazelcastInstance member = factory.newHazelcastInstance(config);
        assertClusterSize(2, member);
    }

    /**
     * <pre>
     * Given: security with a custom member loginmodule is enabled in configuration
     * When: member's login (1st phase) ends with LoginException
     * Then: creating 2nd member fails with an {@link IllegalStateException} thrown
     * </pre>
     */
    @Test
    public void testFailedLogin() {
        Properties properties = new Properties();
        properties.setProperty(PROPERTY_RESULT_LOGIN, VALUE_ACTION_FAIL);
        final Config config = createTestLoginModuleConfig(properties);
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        factory.newHazelcastInstance(config); // master
        expected.expect(IllegalStateException.class);
        factory.newHazelcastInstance(config);
    }

    /**
     * <pre>
     * Given: security with a custom member loginmodule is enabled in configuration
     * When: member's login commit (2nd login phase) ends with LoginException
     * Then: creating 2nd member fails with an {@link IllegalStateException} thrown
     * </pre>
     */
    @Test
    public void testFailedCommit() {
        Properties properties = new Properties();
        properties.setProperty(PROPERTY_PRINCIPALS_SIMPLE, "test");
        properties.setProperty(PROPERTY_RESULT_COMMIT, VALUE_ACTION_FAIL);
        final Config config = createTestLoginModuleConfig(properties);
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        factory.newHazelcastInstance(config); // master
        expected.expect(IllegalStateException.class);
        factory.newHazelcastInstance(config);
    }

    /**
     * <pre>
     * Given: security with 2 login modules is enabled in configuration and the second login module always succeeds
     * When: Usage flag REQUIRED is used for both login modules
     * Then: members form the cluster after successful authentication
     * </pre>
     */
    @Test
    public void testDefaultLoginModuleRequiredPasses() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        final Config config = createLoginModuleStackConfig(LoginModuleUsage.REQUIRED);
        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        assertClusterSize(2, hz1, hz2);
    }

    /**
     * <pre>
     * Given: security with 2 login modules is enabled in configuration and the second login module always succeeds
     * When: Usage flag REQUIRED is used for both login modules
     * Then: after failed authentication creating 2nd member fails with an {@link IllegalStateException} thrown
     * </pre>
     */
    @Test
    public void testDefaultLoginModuleRequiredFails() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        factory.newHazelcastInstance(createLoginModuleStackConfig(LoginModuleUsage.REQUIRED));
        final Config config = createLoginModuleStackConfig(LoginModuleUsage.REQUIRED);
        config.setClusterPassword("anotherPassword");
        expected.expect(IllegalStateException.class);
        factory.newHazelcastInstance(config);
    }

    /**
     * <pre>
     * Given: security with 2 login modules is enabled in configuration and the second login module always succeeds
     * When: Usage flag SUFFICIENT is used for the 1st login module and flag REQUIRED is used for the 2nd
     * Then: even if the authentication fails in the 1st login module, members form the cluster
     * </pre>
     */
    @Test
    public void testDefaultLoginModuleSufficient() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz1 = factory.newHazelcastInstance(createLoginModuleStackConfig(LoginModuleUsage.SUFFICIENT));
        final Config config = createLoginModuleStackConfig(LoginModuleUsage.SUFFICIENT);
        config.setClusterPassword("anotherPassword");
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        assertClusterSize(2, hz1, hz2);
    }

    /**
     * Creates a member configuration with a member login module stack used - {@link DefaultLoginModule} as the first LoginModule and
     * {@link TestLoginModule} as the second.
     *
     * @param usage {@link DefaultLoginModule} flag
     */
    private Config createLoginModuleStackConfig(LoginModuleUsage usage) {
        final Config config = new Config();
        final SecurityConfig secCfg = config.getSecurityConfig();
        Properties properties = new Properties();
        properties.setProperty(PROPERTY_PRINCIPALS_SIMPLE, "testPrincipal");
        secCfg.setEnabled(true);
        secCfg.addMemberLoginModuleConfig(
                new LoginModuleConfig().setClassName(DefaultLoginModule.class.getName()).setUsage(usage));
        secCfg.addMemberLoginModuleConfig(new LoginModuleConfig().setClassName(TestLoginModule.class.getName())
                .setUsage(LoginModuleUsage.REQUIRED).setProperties(properties));
        return config;
    }

    /**
     * Creates member configuration with security enabled and with custom login module for members.
     *
     * @param properties properties of the {@link TestLoginModule} used for members (see constants in {@link TestLoginModule}
     *        for the property names)
     */
    private Config createTestLoginModuleConfig(Properties properties) {
        final Config config = new Config();
        final SecurityConfig secCfg = config.getSecurityConfig();
        secCfg.setEnabled(true);
        LoginModuleConfig loginModuleConfig = new LoginModuleConfig();
        loginModuleConfig.setClassName(TestLoginModule.class.getName());
        loginModuleConfig.setUsage(LoginModuleUsage.REQUIRED);
        loginModuleConfig.setProperties(properties);
        secCfg.addMemberLoginModuleConfig(loginModuleConfig);
        return config;
    }

    @Test(expected = IllegalStateException.class)
    public void testDenyMember() {
        final Config config = new Config();
        final SecurityConfig secCfg = config.getSecurityConfig();
        secCfg.setEnabled(true);

        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        factory.newHazelcastInstance(config); // master
        factory.newHazelcastInstance(new Config());
    }
}
