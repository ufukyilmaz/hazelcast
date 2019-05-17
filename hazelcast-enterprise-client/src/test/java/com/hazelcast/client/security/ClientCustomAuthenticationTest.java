package com.hazelcast.client.security;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.CredentialsFactoryConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.LoginModuleConfig;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.security.ClusterLoginModule;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.CredentialsCallback;
import com.hazelcast.security.ICredentialsFactory;
import com.hazelcast.security.SerializationServiceCallback;
import com.hazelcast.security.TokenCredentials;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginException;

import java.io.IOException;
import java.util.Properties;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category(QuickTest.class)
public class ClientCustomAuthenticationTest extends HazelcastTestSupport {

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

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getSecurityConfig().setCredentials(new CustomCredentials(USER_NAME, KEY1, KEY2));
        hazelcastFactory.newHazelcastClient(clientConfig);
    }

    @Test(expected = IllegalStateException.class)
    public void testMissingCredentials() {
        Config config = getConfig(USER_NAME, KEY1, KEY2);
        hazelcastFactory.newHazelcastInstance(config);

        hazelcastFactory.newHazelcastClient();
    }

    @Test(expected = IllegalStateException.class)
    public void testWrongCredentials() {
        Config config = getConfig(USER_NAME, KEY1, KEY2);
        hazelcastFactory.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getSecurityConfig().setCredentials(new CustomCredentials(USER_NAME, "zzz", "zzz"));
        hazelcastFactory.newHazelcastClient(clientConfig);
    }

    @Test
    public void testCustomCredentialsViaBoth_Credentials_and_CredentialsClassName() {
        Config config = getConfig(USER_NAME, KEY1, KEY2);
        hazelcastFactory.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getSecurityConfig().setCredentials(new CustomCredentials(USER_NAME, KEY1, KEY2));
        clientConfig.getSecurityConfig().setCredentialsClassname(CustomCredentials.class.getName());
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
        ClientConfig clientConfig = new ClientConfig();
        CredentialsFactoryConfig credentialsFactoryConfig = clientConfig.getSecurityConfig().getCredentialsFactoryConfig();
        credentialsFactoryConfig.setProperties(prop);
        credentialsFactoryConfig.setClassName(CustomCredentialsFactory.class.getName());
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

        ClientConfig clientConfig = new ClientConfig();
        CredentialsFactoryConfig credentialsFactoryConfig = clientConfig.getSecurityConfig().getCredentialsFactoryConfig();
        CustomCredentialsFactory customCredentialsFactory = new CustomCredentialsFactory();
        credentialsFactoryConfig.setImplementation(customCredentialsFactory);
        credentialsFactoryConfig.setProperties(prop);
        hazelcastFactory.newHazelcastClient(clientConfig);
    }

    @Test(expected = IllegalStateException.class)
    public void testCustomCredentialsViaBoth_FactoryImplementation_and_Credentials() {
        Config config = getConfig(USER_NAME, KEY1, KEY2);
        hazelcastFactory.newHazelcastInstance(config);

        Properties prop = new Properties();
        prop.setProperty("username", USER_NAME);
        prop.setProperty("key1", KEY1);
        prop.setProperty("key2", KEY2);

        ClientConfig clientConfig = new ClientConfig();
        CredentialsFactoryConfig credentialsFactoryConfig = clientConfig.getSecurityConfig().getCredentialsFactoryConfig();
        CustomCredentialsFactory customCredentialsFactory = new CustomCredentialsFactory();
        credentialsFactoryConfig.setImplementation(customCredentialsFactory);
        credentialsFactoryConfig.setProperties(prop);

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

        ClientConfig clientConfig = new ClientConfig();
        CredentialsFactoryConfig credentialsFactoryConfig = clientConfig.getSecurityConfig().getCredentialsFactoryConfig();
        CustomCredentialsFactory customCredentialsFactory = new CustomCredentialsFactory();
        credentialsFactoryConfig.setImplementation(customCredentialsFactory);
        credentialsFactoryConfig.setClassName(CustomCredentialsFactory.class.getName());
        credentialsFactoryConfig.setProperties(prop);

        hazelcastFactory.newHazelcastClient(clientConfig);
    }

    @Test(expected = IllegalStateException.class)
    public void testCustomCredentialsViaFactoryImplementation_invalidCredentials() {
        Config config = getConfig(USER_NAME, KEY1, KEY2);
        hazelcastFactory.newHazelcastInstance(config);

        Properties prop = new Properties();
        prop.setProperty("username", USER_NAME);
        prop.setProperty("key1", KEY1);
        prop.setProperty("key2", "invalid");

        ClientConfig clientConfig = new ClientConfig();
        CredentialsFactoryConfig credentialsFactoryConfig = clientConfig.getSecurityConfig().getCredentialsFactoryConfig();
        CustomCredentialsFactory customCredentialsFactory = new CustomCredentialsFactory();
        credentialsFactoryConfig.setImplementation(customCredentialsFactory);
        credentialsFactoryConfig.setProperties(prop);
        hazelcastFactory.newHazelcastClient(clientConfig);
    }

    private Config getConfig(String username, String key1, String key2) {
        Config config = new Config();
        config.getSecurityConfig()
                .setEnabled(true)
                .addClientLoginModuleConfig(getLoginModuleConfig(username, key1, key2));
        return config;
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
        public void configure(GroupConfig groupConfig, Properties properties) {
            username = properties.getProperty("username");
            key1 = properties.getProperty("key1");
            key2 = properties.getProperty("key2");
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
            SerializationServiceCallback sscb = new SerializationServiceCallback();
            try {
                callbackHandler.handle(new Callback[] { cb, sscb });
            } catch (IOException | UnsupportedCallbackException e) {
                throw new LoginException("Problem getting credentials");
            }
            Credentials credentials = cb.getCredentials();
            if (credentials instanceof TokenCredentials) {
                TokenCredentials tokenCreds = (TokenCredentials) credentials;
                credentials = sscb.getSerializationService().toObject(tokenCreds.asData());
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
}
