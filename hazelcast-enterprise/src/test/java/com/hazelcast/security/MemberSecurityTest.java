package com.hazelcast.security;

import com.hazelcast.auditlog.AuditableEvent;
import com.hazelcast.config.Config;
import com.hazelcast.config.CredentialsFactoryConfig;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.LoginModuleConfig;
import com.hazelcast.config.LoginModuleConfig.LoginModuleUsage;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.config.security.JaasAuthenticationConfig;
import com.hazelcast.config.security.RealmConfig;
import com.hazelcast.config.security.StaticCredentialsFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.security.auditlog.TestAuditlogService;
import com.hazelcast.security.auditlog.TestAuditlogServiceFactory;
import com.hazelcast.security.loginimpl.DefaultLoginModule;
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

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

import static com.hazelcast.auditlog.AuditlogTypeIds.AUTHENTICATION_MEMBER;
import static com.hazelcast.security.loginmodules.TestLoginModule.PROPERTY_PRINCIPALS_SIMPLE;
import static com.hazelcast.security.loginmodules.TestLoginModule.PROPERTY_RESULT_COMMIT;
import static com.hazelcast.security.loginmodules.TestLoginModule.PROPERTY_RESULT_LOGIN;
import static com.hazelcast.security.loginmodules.TestLoginModule.VALUE_ACTION_FAIL;
import static com.hazelcast.test.AbstractHazelcastClassRunner.getTestMethodName;
import static com.hazelcast.test.Accessors.getAuditlogService;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
        RealmConfig realmConfig = new RealmConfig().setCredentialsFactoryConfig(new CredentialsFactoryConfig()
                .setImplementation(new StaticCredentialsFactory(new UsernamePasswordCredentials("name", "validPass"))));
        Config config1 = new Config();
        config1.getSecurityConfig().setEnabled(true).setMemberRealmConfig("memberRealm", realmConfig);

        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        factory.newHazelcastInstance(config1); // master

        RealmConfig realmConfig2 = new RealmConfig().setCredentialsFactoryConfig(new CredentialsFactoryConfig()
                .setImplementation(new StaticCredentialsFactory(new UsernamePasswordCredentials("name", "invalidPass"))));
        Config config2 = new Config();
        config2.getSecurityConfig().setEnabled(true).setMemberRealmConfig("memberRealm", realmConfig2);

        expected.expect(IllegalStateException.class);
        factory.newHazelcastInstance(config2);
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
        Config config = createTestLoginModuleConfig(properties);
        config.getAuditlogConfig()
            .setEnabled(true)
            .setFactoryClassName(TestAuditlogServiceFactory.class.getName());
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        HazelcastInstance member = factory.newHazelcastInstance(config);
        assertClusterSize(2, member);

        TestAuditlogService auditlog = (TestAuditlogService) getAuditlogService(hz);
        Optional<AuditableEvent> ev = auditlog.getEventQueue().stream().filter(e -> AUTHENTICATION_MEMBER.equals(e.typeId())).findAny();
        assertTrue(ev.isPresent());
        Map<String, Object> parameters = ev.get().parameters();
        assertEquals(Boolean.TRUE, parameters.get("passed"));
        assertThat(parameters.get("credentials"), is(instanceOf(UsernamePasswordCredentials.class)));
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
        Config config = createTestLoginModuleConfig(properties);
        config.getAuditlogConfig()
            .setEnabled(true)
            .setFactoryClassName(TestAuditlogServiceFactory.class.getName());
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        try {
            factory.newHazelcastInstance(config);
            fail("New member start was expected to fail.");
        } catch (IllegalStateException e) {
            ignore(e);
        }
        TestAuditlogService auditlog = (TestAuditlogService) getAuditlogService(hz);
        auditlog.assertEventPresent(AUTHENTICATION_MEMBER);
        Optional<AuditableEvent> ev = auditlog.getEventQueue().stream().filter(e -> AUTHENTICATION_MEMBER.equals(e.typeId())).findAny();
        assertTrue(ev.isPresent());
        Map<String, Object> parameters = ev.get().parameters();
        assertEquals(Boolean.FALSE, parameters.get("passed"));
        assertThat(parameters.get("credentials"), is(instanceOf(UsernamePasswordCredentials.class)));
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
        final Config config = createLoginModuleStackConfig(LoginModuleUsage.REQUIRED, "secret");
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
        factory.newHazelcastInstance(createLoginModuleStackConfig(LoginModuleUsage.REQUIRED, "secret"));
        final Config config = createLoginModuleStackConfig(LoginModuleUsage.REQUIRED, "anotherPassword");
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
        HazelcastInstance hz1 = factory.newHazelcastInstance(createLoginModuleStackConfig(LoginModuleUsage.SUFFICIENT, "secret"));
        final Config config = createLoginModuleStackConfig(LoginModuleUsage.SUFFICIENT, "anotherPassword");
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        assertClusterSize(2, hz1, hz2);
    }

    @Test
    public void testDenyMember() {
        final Config config = smallInstanceConfig();
        final SecurityConfig secCfg = config.getSecurityConfig();
        secCfg.setEnabled(true);

        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        factory.newHazelcastInstance(config); // master
        expected.expect(IllegalStateException.class);
        factory.newHazelcastInstance(new Config());
    }

    /**
     * Check fail-fast approach when member realm is configured to non-existing realm.
     */
    @Test
    public void testRealmDoesntExistMember() {
        final Config config = smallInstanceConfig();
        final SecurityConfig secCfg = config.getSecurityConfig();
        secCfg.setEnabled(true).setMemberRealm("noSuchRealm");
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        expected.expect(InvalidConfigurationException.class);
        factory.newHazelcastInstance(config);
    }

    /**
     * Check fail-fast approach when member realm is configured to non-existing realm.
     */
    @Test
    public void testRealmDoesntExistClient() {
        final Config config = smallInstanceConfig();
        final SecurityConfig secCfg = config.getSecurityConfig();
        secCfg.setEnabled(true).setClientRealm("noSuchRealm");
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        expected.expect(InvalidConfigurationException.class);
        factory.newHazelcastInstance(config);
    }

    /**
     * Checks usage of the {@link HazelcastInstanceCallback} in {@link LoginModule}.
     */
    @Test
    public void testAccessToInstanceNameInLoginModule() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        String name1 = getTestMethodName() + "-1";
        String name2 = getTestMethodName() + "-2";
        assertClusterSizeEventually(2, factory.newHazelcastInstance(createInstanceNameLoginConfig(name1, name1)),
                factory.newHazelcastInstance(createInstanceNameLoginConfig(name2, name2)));
    }

    /**
     * Checks usage of the {@link HazelcastInstanceCallback} in {@link ICredentialsFactory}.
     */
    @Test
    public void testAccessToInstanceNameInCredentialsFactory() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        String name1 = getTestMethodName() + "-1";
        String name2 = getTestMethodName() + "-2";
        assertClusterSizeEventually(2, factory.newHazelcastInstance(createInstanceNameCredsConfig(name1, name1)),
                factory.newHazelcastInstance(createInstanceNameCredsConfig(name2, name2)));
    }

    /**
     * Checks usage of the {@link HazelcastInstanceCallback} in {@link LoginModule}.
     */
    @Test
    public void testInstanceNameLoginModuleAuthnFail() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        String name1 = getTestMethodName() + "-1";
        String name2 = getTestMethodName() + "-2";
        factory.newHazelcastInstance(createInstanceNameLoginConfig(name1, "foo"));
        expected.expect(IllegalStateException.class);
        factory.newHazelcastInstance(createInstanceNameLoginConfig(name2, "bar"));
    }

    /**
     * Checks usage of the {@link HazelcastInstanceCallback} in {@link ICredentialsFactory}.
     */
    @Test
    public void testInstanceNameInCredentialsFactoryFail() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        String name1 = getTestMethodName() + "-1";
        String name2 = getTestMethodName() + "-2";
        factory.newHazelcastInstance(createInstanceNameCredsConfig(name1, "foo"));
        expected.expect(IllegalStateException.class);
        factory.newHazelcastInstance(createInstanceNameCredsConfig(name2, "bar"));
    }

    private Config createInstanceNameLoginConfig(String instanceName, String expectedName) {
        Config config = smallInstanceConfig().setInstanceName(instanceName);
        RealmConfig realmConfig = new RealmConfig().setJaasAuthenticationConfig(new JaasAuthenticationConfig()
                .addLoginModuleConfig(new LoginModuleConfig(InstanceNameLoginModule.class.getName(), LoginModuleUsage.REQUIRED)
                        .setProperty("expected", expectedName)));
        config.getSecurityConfig().setEnabled(true).setMemberRealmConfig("memberRealm", realmConfig);
        return config;
    }

    private Config createInstanceNameCredsConfig(String instanceName, String expectedName) {
        Config config = smallInstanceConfig().setInstanceName(instanceName);
        Properties props = new Properties();
        props.setProperty("expected", expectedName);
        RealmConfig realmConfig = new RealmConfig().setCredentialsFactoryConfig(
                new CredentialsFactoryConfig(InstanceNameCredentialsFactory.class.getName()).setProperties(props));
        config.getSecurityConfig().setEnabled(true).setMemberRealmConfig("memberRealm", realmConfig);
        return config;
    }

    /**
     * Creates a member configuration with a member login module stack used - {@link DefaultLoginModule} as the first LoginModule and
     * {@link TestLoginModule} as the second.
     *
     * @param usage {@link DefaultLoginModule} flag
     * @param password password for the member identity
     */
    private Config createLoginModuleStackConfig(LoginModuleUsage usage, String password) {
        Config config = smallInstanceConfig();
        Properties properties = new Properties();
        properties.setProperty(PROPERTY_PRINCIPALS_SIMPLE, "testPrincipal");
        RealmConfig realmConfig = new RealmConfig().setJaasAuthenticationConfig(new JaasAuthenticationConfig()
                .addLoginModuleConfig(new LoginModuleConfig(DefaultLoginModule.class.getName(), usage))
                .addLoginModuleConfig(new LoginModuleConfig(TestLoginModule.class.getName(), LoginModuleUsage.REQUIRED)
                        .setProperties(properties)))
                .setUsernamePasswordIdentityConfig(getTestMethodName(), password);
        config.getSecurityConfig().setEnabled(true).setMemberRealmConfig("memberRealm", realmConfig);
        return config;
    }

    /**
     * Creates member configuration with security enabled and with custom login module for members.
     *
     * @param properties properties of the {@link TestLoginModule} used for members (see constants in {@link TestLoginModule}
     *        for the property names)
     */
    private Config createTestLoginModuleConfig(Properties properties) {
        final Config config = smallInstanceConfig();
        final SecurityConfig secCfg = config.getSecurityConfig();
        secCfg.setEnabled(true);
        LoginModuleConfig loginModuleConfig = new LoginModuleConfig();
        loginModuleConfig.setClassName(TestLoginModule.class.getName());
        loginModuleConfig.setUsage(LoginModuleUsage.REQUIRED);
        loginModuleConfig.setProperties(properties);
        RealmConfig realmConfig = new RealmConfig().setJaasAuthenticationConfig(new JaasAuthenticationConfig().addLoginModuleConfig(loginModuleConfig));
        secCfg.setMemberRealmConfig("memberRealm", realmConfig);
        return config;
    }

    public static class InstanceNameLoginModule extends ClusterLoginModule {
        String name;

        @Override
        protected boolean onLogin() throws LoginException {
            HazelcastInstanceCallback cb = new HazelcastInstanceCallback();
            try {
                callbackHandler.handle(new Callback[] { cb });
            } catch (IOException | UnsupportedCallbackException e) {
                e.printStackTrace();
            }
            HazelcastInstance hz = cb.getHazelcastInstance();
            name = hz != null ? hz.getName() : null;
            String expected = getStringOption("expected", "");
            if (!expected.equals(name)) {
                throw new FailedLoginException("Unexpected instance name: " + name + ". The expected value is: " + expected);
            }
            return true;
        }

        @Override
        protected String getName() {
            return name;
        }
    }

    public static class InstanceNameCredentialsFactory implements ICredentialsFactory {

        private String name;
        private String expected;

        @Override
        public void init(Properties properties) {
            expected = properties.getProperty("expected", "");
        }

        @Override
        public void configure(CallbackHandler callbackHandler) {
            HazelcastInstanceCallback cb = new HazelcastInstanceCallback();
            try {
                callbackHandler.handle(new Callback[] { cb });
            } catch (IOException | UnsupportedCallbackException e) {
                e.printStackTrace();
            }
            HazelcastInstance hz = cb.getHazelcastInstance();
            name = hz != null ? hz.getName() : null;
        }

        @Override
        public Credentials newCredentials() {
            if (!expected.equals(name)) {
                throw new IllegalStateException("Unexpected instance name: " + name + ". The expected value is: " + expected);
            }
            return new UsernamePasswordCredentials(null, null);
        }

        @Override
        public void destroy() {
        }

    }
}
