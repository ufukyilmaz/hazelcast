package com.hazelcast.client.nio.ssl;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.test.TestAwareClientFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.nio.ssl.OpenSSLEngineFactory;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Properties;

import static com.hazelcast.TestEnvironmentUtil.copyTestResource;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category(QuickTest.class)
public class ClientAuthenticationTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    boolean openSsl;
    private final TestAwareClientFactory factory = new TestAwareClientFactory();

    @After
    public void after() {
        factory.terminateAll();
    }

    // the happy case; everything is working perfect
    @Test
    public void whenRequired() throws Exception {
        TestSettings settings = new TestSettings();
        settings.serverKeystore = "server1_knows_client1/server1.keystore";
        settings.serverTruststore = "server1_knows_client1/server1.truststore";
        settings.clientKeystore = "client1_knows_server1/client1.keystore";
        settings.clientTruststore = "client1_knows_server1/client1.truststore";
        settings.mutualAuthentication = "REQUIRED";
        test(settings);
    }

    // the server isn't authenticated
    @Test(expected = IllegalStateException.class)
    public void whenRequired_andServerNotAuthenticated() throws Exception {
        TestSettings settings = new TestSettings();
        settings.serverKeystore = "server1_knows_client1/server1.keystore";
        settings.serverTruststore = "server1_knows_client1/server1.truststore";
        settings.clientKeystore = "client1_knows_noone/client1.keystore";
        settings.mutualAuthentication = "REQUIRED";
        test(settings);
    }

    // the client isn't authenticated; the server doesn't have any authenticated clients.
    @Test(expected = IllegalStateException.class)
    public void whenRequired_andNoClientsAuthenticated() throws Exception {
        TestSettings settings = new TestSettings();
        settings.serverKeystore = "server1_knows_noone/server1.keystore";
        settings.clientKeystore = "client1_knows_server1/client1.keystore";
        settings.clientTruststore = "client1_knows_server1/client1.truststore";
        settings.mutualAuthentication = "REQUIRED";
        test(settings);
    }

    // the client isn't authenticated; this client isn't known at th server
    @Test(expected = IllegalStateException.class)
    public void whenRequired_andWrongClientAuthenticated() throws Exception {
        TestSettings settings = new TestSettings();
        settings.serverKeystore = "server1_knows_client1/server1.keystore";
        settings.serverTruststore = "server1_knows_client1/server1.keystore";
        settings.clientKeystore = "client2_knows_server1/client2.keystore";
        settings.clientTruststore = "client2_knows_server1/client2.truststore";
        settings.mutualAuthentication = "REQUIRED";
        test(settings);
    }

    // =================================


    // the happy case; everything is working perfect
    @Test
    public void whenOptional() throws Exception {
        TestSettings settings = new TestSettings();
        settings.serverKeystore = "server1_knows_client1/server1.keystore";
        settings.serverTruststore = "server1_knows_client1/server1.truststore";
        settings.clientKeystore = "client1_knows_server1/client1.keystore";
        settings.clientTruststore = "client1_knows_server1/client1.truststore";
        settings.mutualAuthentication = "OPTIONAL";
        test(settings);
    }

    // the server isn't authenticated
    @Test(expected = IllegalStateException.class)
    public void whenOptional_andServerNotAuthenticated() throws Exception {
        TestSettings settings = new TestSettings();
        settings.serverKeystore = "server1_knows_client1/server1.keystore";
        settings.serverTruststore = "server1_knows_client1/server1.truststore";
        settings.clientKeystore = "client1_knows_noone/client1.keystore";
        settings.mutualAuthentication = "OPTIONAL";
        test(settings);
    }

    // the client isn't authenticated; the server doesn't have any authenticated clients.
    // this is not a problem since client authentication is optional
    @Test
    public void whenOptional_andNoClientAuthenticated() throws Exception {
        TestSettings settings = new TestSettings();
        settings.serverKeystore = "server1_knows_noone/server1.keystore";
        settings.clientTruststore = "anonymous_client1_knows_server1/client1.truststore";
        settings.mutualAuthentication = "OPTIONAL";
        test(settings);
    }

    // the client isn't authenticated; this client isn't known at th server
    @Test(expected = IllegalStateException.class)
    public void whenOptional_andWrongClientAuthenticated() throws Exception {
        TestSettings settings = new TestSettings();
        settings.serverKeystore = "server1_knows_client1/server1.keystore";
        settings.serverTruststore = "server1_knows_client1/server1.keystore";
        settings.clientKeystore = "client2_knows_server1/client2.keystore";
        settings.clientTruststore = "client2_knows_server1/client2.truststore";
        settings.mutualAuthentication = "OPTIONAL";
        test(settings);
    }

    // ================= when None

    @Test
    public void whenNone_andServerAuthenticated() throws Exception {
        TestSettings settings = new TestSettings();
        settings.serverKeystore = "server1_knows_noone/server1.keystore";
        settings.clientKeystore = "client1_knows_server1/client1.keystore";
        settings.clientTruststore = "client1_knows_server1/client1.truststore";
        test(settings);
    }

    // the server isn't authenticated
    @Test(expected = IllegalStateException.class)
    public void whenNone_andServerNotAuthenticated() throws Exception {
        TestSettings settings = new TestSettings();
        settings.serverKeystore = "server1_knows_noone/server1.keystore";
        settings.clientKeystore = "client1_knows_noone/client1.keystore";
        test(settings);
    }

    public void test(TestSettings settings) throws Exception {
        Properties serverProps = new Properties();
        if (settings.serverKeystore != null) {
            serverProps.setProperty("javax.net.ssl.keyStore", makeFile(settings.serverKeystore));
        }
        if (settings.serverTruststore != null) {
            serverProps.setProperty("javax.net.ssl.trustStore", makeFile(settings.serverTruststore));
        }
        serverProps.setProperty("javax.net.ssl.keyStorePassword", "password");
        serverProps.setProperty("javax.net.ssl.trustStorePassword", "password");
        serverProps.setProperty("javax.net.ssl.protocol", "TLS");
        if (settings.mutualAuthentication != null) {
            serverProps.setProperty("javax.net.ssl.mutualAuthentication", settings.mutualAuthentication);
        }

        Config config = smallInstanceConfig().setProperty(GroupProperty.IO_THREAD_COUNT.getName(), "1");
        NetworkConfig networkConfig = config.getNetworkConfig();
        networkConfig.setSSLConfig(new SSLConfig()
                .setEnabled(true)
                .setProperties(serverProps));
        if (openSsl) {
            networkConfig.getSSLConfig().setFactoryImplementation(new OpenSSLEngineFactory());
        }
        factory.newHazelcastInstance(config);

        Properties clientProps = new Properties();
        if (settings.clientKeystore != null) {
            clientProps.setProperty("javax.net.ssl.keyStore", makeFile(settings.clientKeystore));
        }
        if (settings.clientTruststore != null) {
            clientProps.setProperty("javax.net.ssl.trustStore", makeFile(settings.clientTruststore));
        }
        clientProps.setProperty("javax.net.ssl.keyStorePassword", "password");
        clientProps.setProperty("javax.net.ssl.trustStorePassword", "password");
        serverProps.setProperty("javax.net.ssl.protocol", "TLS");
        ClientConfig clientConfig = new ClientConfig();
        ClientNetworkConfig clientNetworkConfig = clientConfig.getNetworkConfig()
                .setConnectionAttemptLimit(1)
                .setSSLConfig(new SSLConfig()
                        .setEnabled(true)
                        .setProperties(clientProps));
        if (openSsl) {
            clientNetworkConfig.getSSLConfig().setFactoryImplementation(new OpenSSLEngineFactory());
        }

        HazelcastInstance client = factory.newHazelcastClient(clientConfig);
        client.getCPSubsystem().getAtomicLong("foo").incrementAndGet();
    }

    private String makeFile(String relativePath) throws IOException {
        String resourceName = "/com/hazelcast/nio/ssl-mutual-auth/" + relativePath;
        return copyTestResource(ClientAuthenticationTest.class, tempFolder.getRoot(),
                resourceName).getAbsolutePath();
    }

    static class TestSettings {
        String serverKeystore;
        String serverTruststore;
        String clientKeystore;
        String clientTruststore;
        String mutualAuthentication;
    }
}
