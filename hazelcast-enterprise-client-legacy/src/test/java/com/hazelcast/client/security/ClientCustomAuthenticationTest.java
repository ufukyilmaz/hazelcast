package com.hazelcast.client.security;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.client.ClientPortableFactory;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.LoginModuleConfig;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.ClusterLoginModule;
import com.hazelcast.security.Credentials;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.util.Properties;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class ClientCustomAuthenticationTest extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    private final static String username = "user";
    private final static String key1 = "abc";
    private final static String key2 = "xyz";

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testCustomCredentials() {
        Config config = getConfig(username, key1, key2);
        hazelcastFactory.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getSecurityConfig().setCredentials(new CustomCredentials(username, key1, key2));
        hazelcastFactory.newHazelcastClient(clientConfig);
    }

    @Test(expected = IllegalStateException.class)
    public void testMissingCredentials() {
        Config config = getConfig(username, key1, key2);
        hazelcastFactory.newHazelcastInstance(config);

        hazelcastFactory.newHazelcastClient();
    }

    @Test(expected = IllegalStateException.class)
    public void testWrongCredentials() {
        Config config = getConfig(username, key1, key2);
        hazelcastFactory.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getSecurityConfig().setCredentials(new CustomCredentials(username, "zzz", "zzz"));
        hazelcastFactory.newHazelcastClient(clientConfig);
    }

    private Config getConfig(String username, String key1, String key2) {
        Config config = new Config();
        config.getSerializationConfig()
                .addPortableFactory(1, new PortableFactory() {
                    @Override
                    public Portable create(int classId) {
                        return new CustomCredentials();
                    }
                });
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

    public static class CustomCredentials implements Credentials, Portable {

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
        public String getEndpoint() {
            return endpoint;
        }

        @Override
        public void setEndpoint(String endpoint) {
            this.endpoint = endpoint;
        }

        @Override
        public String getPrincipal() {
            return username;
        }

        public String getKey1() {
            return key1;
        }

        public String getKey2() {
            return key2;
        }

        @Override
        public int getFactoryId() {
            return 1;
        }

        @Override
        public int getClassId() {
            return 1;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeUTF("username", username);
            writer.writeUTF("key1", key1);
            writer.writeUTF("key2", key2);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            username = reader.readUTF("username");
            key1 = reader.readUTF("key1");
            key2 = reader.readUTF("key2");
        }
    }

    public static class CustomLoginModule extends ClusterLoginModule {
        @Override
        protected boolean onLogin() throws LoginException {
            if (!(credentials instanceof CustomCredentials)) {
                return false;
            }
            CustomCredentials cc = (CustomCredentials) credentials;
            if (cc.getPrincipal().equals(options.get("username")) &&
                    cc.getKey1().equals(options.get("key1")) &&
                    cc.getKey2().equals(options.get("key2"))) {
                return true;
            }
            throw new LoginException("Invalid credentials");
        }

        @Override
        protected boolean onCommit() throws LoginException {
            return loginSucceeded;
        }

        @Override
        protected boolean onAbort() throws LoginException {
            return false;
        }

        @Override
        protected boolean onLogout() throws LoginException {
            return false;
        }
    }
}
