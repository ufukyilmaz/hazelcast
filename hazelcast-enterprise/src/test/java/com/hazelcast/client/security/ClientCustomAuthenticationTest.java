package com.hazelcast.client.security;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.ClientEndpoint;
import com.hazelcast.client.impl.ClientEngineImpl;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.CredentialsFactoryConfig;
import com.hazelcast.config.LoginModuleConfig;
import com.hazelcast.config.security.JaasAuthenticationConfig;
import com.hazelcast.config.security.RealmConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.security.ClusterLoginModule;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.CredentialsCallback;
import com.hazelcast.security.ICredentialsFactory;
import com.hazelcast.security.TokenCredentials;
import com.hazelcast.security.TokenDeserializerCallback;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category(QuickTest.class)
public class ClientCustomAuthenticationTest extends ClientTestSupport {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    private static final String USER_NAME = "user";
    private static final String KEY1 = "abc";
    private static final String KEY2 = "xyz";

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testCustomCredentials() {
        Config config = getConfig(USER_NAME, KEY1, KEY2);
        hazelcastFactory.newHazelcastInstance(config);

        ClientConfig clientConfig = failFastClientConfig();
        clientConfig.getSecurityConfig().setCredentials(new CustomCredentials(USER_NAME, KEY1, KEY2));
        hazelcastFactory.newHazelcastClient(clientConfig);
    }

    @Test
    public void testMissingCredentials() {
        Config config = getConfig(USER_NAME, KEY1, KEY2);
        hazelcastFactory.newHazelcastInstance(config);
        expected.expect(IllegalStateException.class);
        hazelcastFactory.newHazelcastClient(failFastClientConfig());
    }

    @Test
    public void testWrongCredentials() {
        Config config = getConfig(USER_NAME, KEY1, KEY2);
        hazelcastFactory.newHazelcastInstance(config);

        ClientConfig clientConfig = failFastClientConfig();
        clientConfig.getSecurityConfig().setCredentials(new CustomCredentials(USER_NAME, "zzz", "zzz"));
        expected.expect(IllegalStateException.class);
        hazelcastFactory.newHazelcastClient(clientConfig);
    }

    @Test
    public void testCustomCredentialsViaBoth_Credentials_and_CredentialsClassName() {
        Config config = getConfig(USER_NAME, KEY1, KEY2);
        hazelcastFactory.newHazelcastInstance(config);

        ClientConfig clientConfig = failFastClientConfig();
        clientConfig.getSecurityConfig().setCredentials(new CustomCredentials(USER_NAME, KEY1, KEY2));
        hazelcastFactory.newHazelcastClient(clientConfig);
    }

    @Test
    public void testCustomCredentialsViaFactoryClassName() {
        Config config = getConfig(USER_NAME, KEY1, KEY2);
        hazelcastFactory.newHazelcastInstance(config);

        Properties prop = new Properties();
        prop.setProperty("username", USER_NAME);
        prop.setProperty("key1", KEY1);
        prop.setProperty("key2", KEY2);
        ClientConfig clientConfig = failFastClientConfig();
        clientConfig.getSecurityConfig().setCredentialsFactoryConfig(
                new CredentialsFactoryConfig(CustomCredentialsFactory.class.getName()).setProperties(prop));
        hazelcastFactory.newHazelcastClient(clientConfig);
    }

    @Test
    public void testCustomCredentialsViaFactoryImplementation() {
        Config config = getConfig(USER_NAME, KEY1, KEY2);
        hazelcastFactory.newHazelcastInstance(config);

        Properties prop = new Properties();
        prop.setProperty("username", USER_NAME);
        prop.setProperty("key1", KEY1);
        prop.setProperty("key2", KEY2);

        ClientConfig clientConfig = failFastClientConfig();
        CustomCredentialsFactory credsFactory = new CustomCredentialsFactory();
        credsFactory.init(prop);
        clientConfig.getSecurityConfig().setCredentialsFactoryConfig(
                new CredentialsFactoryConfig().setImplementation(credsFactory));
        hazelcastFactory.newHazelcastClient(clientConfig);
    }

    @Test
    public void testCustomCredentialsViaBoth_FactoryImplementation_and_Credentials() {
        Config config = getConfig(USER_NAME, KEY1, KEY2);
        hazelcastFactory.newHazelcastInstance(config);

        Properties prop = new Properties();
        prop.setProperty("username", USER_NAME);
        prop.setProperty("key1", KEY1);
        prop.setProperty("key2", KEY2);

        ClientConfig clientConfig = failFastClientConfig();
        clientConfig.getSecurityConfig().setCredentialsFactoryConfig(
                new CredentialsFactoryConfig(CustomCredentialsFactory.class.getName()).setProperties(prop));

        clientConfig.getSecurityConfig().setCredentials(new CustomCredentials(USER_NAME, KEY1, KEY2));

        hazelcastFactory.newHazelcastClient(clientConfig);
    }


    @Test
    public void testCustomCredentialsViaBoth_FactoryImplementation_and_FactoryClassName() {
        Config config = getConfig(USER_NAME, KEY1, KEY2);
        hazelcastFactory.newHazelcastInstance(config);

        Properties prop = new Properties();
        prop.setProperty("username", USER_NAME);
        prop.setProperty("key1", KEY1);
        prop.setProperty("key2", KEY2);

        ClientConfig clientConfig = failFastClientConfig();
        CustomCredentialsFactory credsFactory = new CustomCredentialsFactory();
        credsFactory.init(prop);
        CredentialsFactoryConfig credFactoryConfig = new CredentialsFactoryConfig(CustomCredentialsFactory.class.getName())
                .setProperties(prop)
                .setImplementation(credsFactory);
        clientConfig.getSecurityConfig()
                .setCredentialsFactoryConfig(credFactoryConfig);

        hazelcastFactory.newHazelcastClient(clientConfig);
    }

    @Test
    public void testCustomCredentialsViaFactoryImplementation_invalidCredentials() {
        Config config = getConfig(USER_NAME, KEY1, KEY2);
        hazelcastFactory.newHazelcastInstance(config);

        Properties prop = new Properties();
        prop.setProperty("username", USER_NAME);
        prop.setProperty("key1", KEY1);
        prop.setProperty("key2", "invalid");

        ClientConfig clientConfig = failFastClientConfig();
        CustomCredentialsFactory credsFactory = new CustomCredentialsFactory();
        credsFactory.init(prop);
        clientConfig.getSecurityConfig().setCredentialsFactoryConfig(
                new CredentialsFactoryConfig().setImplementation(credsFactory));
        expected.expect(IllegalStateException.class);
        hazelcastFactory.newHazelcastClient(clientConfig);
    }

    private Config getConfig(String username, String key1, String key2) {
        Config config = new Config();
        config.getSecurityConfig()
                .setEnabled(true)
                .setClientRealmConfig("realm",
                        new RealmConfig().setJaasAuthenticationConfig(
                                new JaasAuthenticationConfig().addLoginModuleConfig(getLoginModuleConfig(username, key1, key2))));
        return config;
    }

    private ClientConfig failFastClientConfig() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(0);
        return clientConfig;
    }

    private LoginModuleConfig getLoginModuleConfig(String username, String key1, String key2) {
        Properties prop = new Properties();
        prop.setProperty("username", username);
        prop.setProperty("key1", key1);
        prop.setProperty("key2", key2);
        return new LoginModuleConfig()
                .setUsage(LoginModuleConfig.LoginModuleUsage.REQUIRED)
                .setClassName(CustomLoginModule.class.getName())
                .setProperties(prop);
    }

    public static class CustomCredentialsFactory implements ICredentialsFactory {

        private String username;
        private String key1;
        private String key2;

        @Override
        public void init(Properties properties) {
            username = properties.getProperty("username");
            key1 = properties.getProperty("key1");
            key2 = properties.getProperty("key2");
        }

        @Override
        public void configure(CallbackHandler callbackHandler) {
        }

        @Override
        public Credentials newCredentials() {
            return new CustomCredentials(username, key1, key2);
        }

        @Override
        public void destroy() {

        }
    }

    public static class CustomCredentials implements Credentials {

        private String username;
        private String key1;
        private String key2;

        public CustomCredentials() {

        }

        public CustomCredentials(String username, String key1, String key2) {
            this.username = username;
            this.key1 = key1;
            this.key2 = key2;
        }

        private String endpoint;

        @Override
        public String getName() {
            return username;
        }

        public String getKey1() {
            return key1;
        }

        public String getKey2() {
            return key2;
        }
    }

    public static class CustomLoginModule extends ClusterLoginModule {
        private String name;

        @Override
        protected boolean onLogin() throws LoginException {
            CredentialsCallback cb = new CredentialsCallback();
            TokenDeserializerCallback tdcb = new TokenDeserializerCallback();
            try {
                callbackHandler.handle(new Callback[]{cb, tdcb});
            } catch (IOException | UnsupportedCallbackException e) {
                throw new LoginException("Problem getting credentials");
            }
            Credentials credentials = cb.getCredentials();
            if (credentials instanceof TokenCredentials) {
                TokenCredentials tokenCreds = (TokenCredentials) credentials;
                credentials = (Credentials) tdcb.getTokenDeserializer().deserialize(tokenCreds);
            }
            if (!(credentials instanceof CustomCredentials)) {
                throw new FailedLoginException();
            }
            CustomCredentials cc = (CustomCredentials) credentials;
            if (cc.getName().equals(options.get("username"))
                    && cc.getKey1().equals(options.get("key1"))
                    && cc.getKey2().equals(options.get("key2"))) {
                name = cc.getName();
                addRole(name);
                return true;
            }
            throw new LoginException("Invalid credentials");
        }

        @Override
        protected String getName() {
            return name;
        }
    }

    public static class LogoutThrowingExceptionModule extends ClusterLoginModule {

        @Override
        protected boolean onLogin() {
            return true;
        }

        @Override
        protected String getName() {
            return "test";
        }

        @Override
        protected boolean onLogout() throws LoginException {
            throw new LoginException();
        }
    }

    @Test
    public void testLoginModuleThrowsExceptionOnLogout() {
        Config config = new Config();

        LoginModuleConfig loginModuleConfig = new LoginModuleConfig();
        loginModuleConfig.setUsage(LoginModuleConfig.LoginModuleUsage.REQUIRED).setClassName(LogoutThrowingExceptionModule.class.getName());
        JaasAuthenticationConfig jaasAuthenticationConfig = new JaasAuthenticationConfig();
        jaasAuthenticationConfig.addLoginModuleConfig(loginModuleConfig);
        RealmConfig realmConfig = new RealmConfig();
        realmConfig.setJaasAuthenticationConfig(jaasAuthenticationConfig);
        config.getSecurityConfig()
                .setEnabled(true)
                .setClientRealmConfig("realm", realmConfig);

        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance(config);

        HazelcastInstance client = hazelcastFactory.newHazelcastClient(failFastClientConfig());
        client.shutdown();

        assertTrueEventually(() -> {
            ClientEngineImpl clientEngine = getClientEngineImpl(instance);
            Map<ClientEndpoint, Long> endpoints = clientEngine.getClusterListenerService().getClusterListeningEndpoints();

            assertEquals(0, clientEngine.getClientEndpointCount());
            assertEquals(0, endpoints.size());
        });

    }
}
