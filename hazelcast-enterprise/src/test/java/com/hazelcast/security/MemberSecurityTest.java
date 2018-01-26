package com.hazelcast.security;

import com.hazelcast.config.Config;
import com.hazelcast.config.CredentialsFactoryConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.LoginModuleConfig;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.config.LoginModuleConfig.LoginModuleUsage;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.loginmodules.TestLoginModule;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.security.loginmodules.TestLoginModule.PROPERTY_PRINCIPALS_SIMPLE;
import static com.hazelcast.security.loginmodules.TestLoginModule.PROPERTY_RESULT_COMMIT;
import static com.hazelcast.security.loginmodules.TestLoginModule.PROPERTY_RESULT_LOGIN;
import static com.hazelcast.security.loginmodules.TestLoginModule.VALUE_ACTION_FAIL;

import java.util.Properties;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MemberSecurityTest extends HazelcastTestSupport {

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

    @Test(expected = IllegalStateException.class)
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
            public void configure(GroupConfig groupConfig, Properties properties) {
            }
        });
        secCfg.setMemberCredentialsConfig(credentialsFactoryConfig);

        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        factory.newHazelcastInstance(config); // master
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
    @Test(expected = IllegalStateException.class)
    public void testFailedLogin() {
        Properties properties = new Properties();
        properties.setProperty(PROPERTY_RESULT_LOGIN, VALUE_ACTION_FAIL);
        final Config config = createTestLoginModuleConfig(properties);
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        factory.newHazelcastInstance(config); // master
        factory.newHazelcastInstance(config);
    }

    /**
     * <pre>
     * Given: security with a custom member loginmodule is enabled in configuration
     * When: member's login commit (2nd login phase) ends with LoginException
     * Then: creating 2nd member fails with an {@link IllegalStateException} thrown
     * </pre>
     */
    @Test(expected = IllegalStateException.class)
    public void testFailedCommit() {
        Properties properties = new Properties();
        properties.setProperty(PROPERTY_PRINCIPALS_SIMPLE, "test");
        properties.setProperty(PROPERTY_RESULT_COMMIT, VALUE_ACTION_FAIL);
        final Config config = createTestLoginModuleConfig(properties);
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        factory.newHazelcastInstance(config); // master
        factory.newHazelcastInstance(config);
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

    public static class InValidCredentials extends AbstractCredentials {

        public InValidCredentials() {
            super("invalid-group-name");
        }

        @Override
        protected void writePortableInternal(PortableWriter writer) {
        }

        @Override
        protected void readPortableInternal(PortableReader reader) {
        }

        @Override
        public int getFactoryId() {
            return 1234;
        }

        @Override
        public int getClassId() {
            return 1;
        }
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
