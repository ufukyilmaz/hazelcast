package com.hazelcast.internal.ascii;

import com.hazelcast.SimpleTlsProxy;
import com.hazelcast.config.Config;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.SlowTest;
import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.FailureMode;
import net.spy.memcached.MemcachedClient;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.instance.EndpointQualifier.MEMCACHE;
import static com.hazelcast.instance.EndpointQualifier.REST;
import static com.hazelcast.nio.ssl.SSLEngineFactorySupport.loadKeyManagerFactory;
import static com.hazelcast.nio.ssl.SSLEngineFactorySupport.loadTrustManagerFactory;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.JAVAX_NET_SSL_KEY_STORE;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.JAVAX_NET_SSL_KEY_STORE_PASSWORD;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.JAVAX_NET_SSL_TRUST_STORE;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.getOrCreateTempFile;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.keyStore;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.trustStore;
import static com.hazelcast.test.HazelcastTestSupport.ignore;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static java.util.Arrays.asList;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseSerialParametersRunnerFactory.class)
@Category(SlowTest.class)
public class EnterpriseInvalidEndpointTest
        extends InvalidEndpointTest {

    private final String KEY_STORE_PATH = getOrCreateTempFile(keyStore);
    private final String TRUST_STORE_PATH = getOrCreateTempFile(trustStore);
    private final String STORE_PWD = "123456";

    @Parameterized.Parameters(name = "tls:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {false},
                {true}
        });
    }

    @Parameterized.Parameter(value = 0)
    public boolean tls = true;
    @Test
    public void attemptHttpOnMemcacheEndpoint() {
        Config config = createMemcacheEndpointConfig();
        if (tls) {
            config.getAdvancedNetworkConfig().getEndpointConfigs().get(MEMCACHE).setSSLConfig(createSSLConfig());
        }

        HazelcastInstance instance = factory.newHazelcastInstance(config);

        // Invalid endpoint - points to MEMCACHE
        String address = instance.getCluster().getLocalMember().getSocketAddress(MEMCACHE).toString();
        String url = (tls ? "https:/" : "http:/") + address + "/management/cluster/version";
        try {
            doHttpGet(url);
            fail("Should not be able to connect");
        } catch (IOException e) {
            ignore(e);
        }
    }

    @Test
    public void attemptMemcacheOnHttpEndpoint()
            throws IOException, InterruptedException {

        Config config = createRestEndpointConfig();
        if (tls) {
            config.getAdvancedNetworkConfig().getRestEndpointConfig().setSSLConfig(createSSLConfig());
        }

        HazelcastInstance instance = factory.newHazelcastInstance(config);

        SimpleTlsProxy proxy = null;
        try {
            // Invalid endpoint - points to REST
            InetSocketAddress address;
            if (tls) {
                InetSocketAddress target = instance.getCluster().getLocalMember().getSocketAddress(REST);
                proxy = new SimpleTlsProxy(8192, KEY_STORE_PATH, STORE_PWD, TRUST_STORE_PATH, STORE_PWD, target, 1);
                proxy.start();

                address = proxy.getProxyAddress();
            } else {
                address = instance.getCluster().getLocalMember().getSocketAddress(REST);
            }

            ConnectionFactory factory =
                    new ConnectionFactoryBuilder().setOpTimeout(60 * 60 * 60).setDaemon(true).setFailureMode(FailureMode.Retry).build();
            MemcachedClient client = new MemcachedClient(factory, Collections.singletonList(address));

            try {
                client.set("one", 0, "two").get();
                fail("Should not be able to connect");
            } catch (InterruptedException e) {
                ignore(e);
            } catch (ExecutionException e) {
                ignore(e);
            }
        } finally {
            if (proxy != null) {
                proxy.stop();
            }
        }
    }

    private SSLConfig createSSLConfig() {
        Properties props = new Properties();
        props.setProperty(JAVAX_NET_SSL_KEY_STORE, KEY_STORE_PATH);
        props.setProperty(JAVAX_NET_SSL_TRUST_STORE, TRUST_STORE_PATH);
        props.setProperty(JAVAX_NET_SSL_KEY_STORE_PASSWORD, STORE_PWD);

        SSLConfig sslConfig = new SSLConfig();
        sslConfig.setEnabled(true);
        sslConfig.setProperties(props);

        return sslConfig;
    }

    @Override
    protected CloseableHttpClient newHttpClient()
            throws IOException {
        HttpClientBuilder builder = HttpClients.custom();

        if (tls) {
            SSLContext sslContext;
            try {
                sslContext = SSLContext.getInstance("TLS");
            } catch (NoSuchAlgorithmException e) {
                throw new IOException(e);
            }

            KeyManagerFactory kmf;
            TrustManagerFactory tmf;
            try {
                kmf = loadKeyManagerFactory(STORE_PWD, KEY_STORE_PATH, KeyManagerFactory.getDefaultAlgorithm());
                tmf = loadTrustManagerFactory(STORE_PWD, TRUST_STORE_PATH, TrustManagerFactory.getDefaultAlgorithm());
            } catch (Exception e) {
                throw rethrow(e);
            }

            try {
                sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());
            } catch (KeyManagementException e) {
                throw new IOException(e);
            }

            builder.setSSLSocketFactory(new SSLConnectionSocketFactory(sslContext,
                    SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER));
        }

        return builder.build();
    }
}
