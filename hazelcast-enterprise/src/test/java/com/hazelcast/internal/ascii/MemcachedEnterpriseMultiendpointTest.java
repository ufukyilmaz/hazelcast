package com.hazelcast.internal.ascii;

import com.hazelcast.SimpleTlsProxy;
import com.hazelcast.config.AdvancedNetworkConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.ServerSocketEndpointConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Properties;

import static com.hazelcast.instance.EndpointQualifier.MEMCACHE;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.JAVAX_NET_SSL_KEY_STORE;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.JAVAX_NET_SSL_KEY_STORE_PASSWORD;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.JAVAX_NET_SSL_TRUST_STORE;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.getOrCreateTempFile;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.keyStore;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.trustStore;
import static java.util.Arrays.asList;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseSerialParametersRunnerFactory.class)
@Category(QuickTest.class)
public class MemcachedEnterpriseMultiendpointTest
        extends MemcachedMultiendpointTest {

    @Parameterized.Parameters(name = "tls:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {false},
                {true}
        });
    }

    @Parameterized.Parameter(value = 0)
    public boolean tls;

    private SimpleTlsProxy proxy;

    @Before
    @Override
    public void setup() throws Exception {
        instance = Hazelcast.newHazelcastInstance(createConfig());

        if (tls) {
            InetSocketAddress target = instance.getCluster().getLocalMember().getSocketAddress(MEMCACHE);
            proxy = new SimpleTlsProxy(8192, getOrCreateTempFile(keyStore), "123456",
                    getOrCreateTempFile(trustStore), "123456", target, 1);
            proxy.start();
        }

        client = getMemcachedClient(instance);
    }

    @After
    @Override
    public void tearDown() {
        super.tearDown();
        if (proxy != null) {
            try {
                proxy.stop();
            } catch (InterruptedException e) {
                ignore(e);
            }
        }
    }

    @Override
    protected Config createConfig() {
        Config config = smallInstanceConfig();
        AdvancedNetworkConfig anc = config.getAdvancedNetworkConfig();
        ServerSocketEndpointConfig serverSocketEndpointConfig = new ServerSocketEndpointConfig();

        anc.setEnabled(true)
           .setMemcacheEndpointConfig(serverSocketEndpointConfig);

        if (tls) {
            Properties props = new Properties();
            props.setProperty(JAVAX_NET_SSL_KEY_STORE, getOrCreateTempFile(keyStore));
            props.setProperty(JAVAX_NET_SSL_TRUST_STORE, getOrCreateTempFile(trustStore));
            props.setProperty(JAVAX_NET_SSL_KEY_STORE_PASSWORD, "123456");

            serverSocketEndpointConfig.setSSLConfig(
                    new SSLConfig().setEnabled(true).setProperties(props));
        }
        // Join is disabled intentionally. will start standalone HazelcastInstances.
        JoinConfig join = anc.getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        return config;
    }

    @Override
    protected InetSocketAddress getMemcachedAddr(HazelcastInstance instance) {
        if (tls) {
            return proxy.getProxyAddress();
        }

        return super.getMemcachedAddr(instance);
    }

}
