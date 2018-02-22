package com.hazelcast.client.nio.ssl;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.nio.ssl.OpenSSLEngineFactory;
import com.hazelcast.nio.ssl.TestKeyStoreUtil;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static com.hazelcast.nio.IOUtil.closeResource;
import static java.io.File.createTempFile;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class ClientAuthenticationTest {

    boolean openSsl;

    @Before
    @After
    public void killAllHazelcastInstances() {
        HazelcastInstanceFactory.terminateAll();
        HazelcastClient.shutdownAll();
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

        Config config = new Config()
                .setProperty(GroupProperty.IO_THREAD_COUNT.getName(), "1");
        NetworkConfig networkConfig = config.getNetworkConfig();
        networkConfig.getJoin().getMulticastConfig()
                .setEnabled(false);
        networkConfig.getJoin().getTcpIpConfig()
                .setEnabled(true)
                .addMember("127.0.0.1")
                .setConnectionTimeoutSeconds(3000);
        networkConfig.setSSLConfig(new SSLConfig()
                .setEnabled(true)
                .setProperties(serverProps));
        if (openSsl) {
            networkConfig.getSSLConfig().setFactoryImplementation(new OpenSSLEngineFactory());
        }

        Hazelcast.newHazelcastInstance(config);

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

        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        client.getAtomicLong("foo").incrementAndGet();
    }

    public static String makeFile(String relativePath) throws IOException {
        String resourcePath = "com/hazelcast/nio/ssl-mutual-auth/" + relativePath;
        ClassLoader cl = TestKeyStoreUtil.class.getClassLoader();
        InputStream resourceAsStream = cl.getResourceAsStream(resourcePath);
        if (resourceAsStream == null) {
            throw new RuntimeException("Failed to locate [" + resourcePath + "]");
        }
        InputStream in = null;
        BufferedOutputStream out = null;
        try {
            File file = createTempFile("hazelcast", "jks");
            file.deleteOnExit();

            in = new BufferedInputStream(resourceAsStream);
            out = new BufferedOutputStream(new FileOutputStream(file));

            int b;
            while ((b = in.read()) > -1) {
                out.write(b);
            }

            out.flush();
            return file.getAbsolutePath();
        } finally {
            closeResource(out);
            closeResource(in);
        }
    }

    static class TestSettings {
        String serverKeystore;
        String serverTruststore;
        String clientKeystore;
        String clientTruststore;
        String mutualAuthentication;
    }
}
