package com.hazelcast.client.nio.ssl;

import com.hazelcast.client.test.TestAwareClientFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.internal.ascii.HTTPCommunicator;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import javax.net.ssl.TrustManagerFactory;

import java.io.IOException;
import java.util.Properties;

import static com.hazelcast.TestEnvironmentUtil.copyTestResource;
import static com.hazelcast.nio.ssl.SSLEngineFactorySupport.loadTrustManagerFactory;
import static com.hazelcast.spi.properties.GroupProperty.REST_ENABLED;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class HttpsRestTest extends HazelcastTestSupport {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private final TestAwareClientFactory factory = new TestAwareClientFactory();

    @After
    public void after() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void test() throws Exception {
        Properties serverProps = new Properties();
        serverProps.setProperty("javax.net.ssl.keyStore", makeFile("server1_knows_noone/server1.keystore"));
        serverProps.setProperty("javax.net.ssl.keyStorePassword", "password");
        serverProps.setProperty("javax.net.ssl.trustStorePassword", "password");
        serverProps.setProperty("javax.net.ssl.protocol", "TLS");

        Config config = new Config()
                .setProperty(REST_ENABLED.getName(), "true")
                .setProperty(GroupProperty.IO_THREAD_COUNT.getName(), "1");
        NetworkConfig networkConfig = config.getNetworkConfig();
        networkConfig.setSSLConfig(new SSLConfig()
                .setEnabled(true)
                .setProperties(serverProps));
        HazelcastInstance instance = factory.newHazelcastInstance(config);

        TrustManagerFactory trustManagerFactory = loadTrustManagerFactory(
                "password",
                makeFile("anonymous_client1_knows_server1/client1.truststore"),
                TrustManagerFactory.getDefaultAlgorithm());

        HTTPCommunicator communicator = new HTTPCommunicator(instance)
                .setClientTrustManagers(trustManagerFactory)
                .setTlsProtocol(serverProps.getProperty("javax.net.ssl.protocol"));
        String name = randomMapName();

        String key = "key";
        String value = "value";

        assertEquals(HTTP_OK, communicator.mapPut(name, key, value));
        assertEquals(value, communicator.mapGetAndResponse(name, key));
        assertTrue(instance.getMap(name).containsKey(key));
    }


    private String makeFile(String relativePath) throws IOException {
        String resourceName = "/com/hazelcast/nio/ssl-mutual-auth/" + relativePath;
        return copyTestResource(ClientAuthenticationTest.class, tempFolder.getRoot(),
                resourceName).getAbsolutePath();
    }
}
