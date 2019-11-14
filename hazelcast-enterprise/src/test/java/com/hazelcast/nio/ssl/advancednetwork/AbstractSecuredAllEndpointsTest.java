package com.hazelcast.nio.ssl.advancednetwork;

import com.hazelcast.config.Config;
import com.hazelcast.config.RestServerEndpointConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.ServerSocketEndpointConfig;
import com.hazelcast.core.Hazelcast;
import org.junit.Before;

import java.util.Properties;

import static com.hazelcast.nio.ssl.TestKeyStoreUtil.JAVAX_NET_SSL_KEY_STORE;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.JAVAX_NET_SSL_KEY_STORE_PASSWORD;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;

public abstract class AbstractSecuredAllEndpointsTest extends AbstractSecuredEndpointsTest {

    @Before
    public void setup() {
        Config config = createCompleteMultiSocketConfigWithSecurity();
        configureTcpIpConfig(config);
        hz = Hazelcast.newHazelcastInstance(config);
    }

    private Config createCompleteMultiSocketConfigWithSecurity() {
        Config config = smallInstanceConfig();
        config.getAdvancedNetworkConfig().setEnabled(true)
                .setMemberEndpointConfig(createServerSocketConfig(MEMBER_PORT,
                        prepareSslPropertiesWithTrustStore(memberAKeystore, MEMBER_PASSWORD, memberATruststore,
                                MEMBER_PASSWORD)))
                .setClientEndpointConfig(createServerSocketConfig(CLIENT_PORT,
                        prepareSslProperties(memberForClientKeystore, CLIENT_PASSWORD)))
                .addWanEndpointConfig(createServerSocketConfig(WAN_PORT, "WAN",
                        prepareSslProperties(memberForWanKeystore, WAN_PASSWORD)))
                .setRestEndpointConfig(createRestServerSocketConfig(REST_PORT, "REST",
                        prepareSslProperties(memberForRestKeystore, REST_PASSWORD)))
                .setMemcacheEndpointConfig(createServerSocketConfig(MEMCACHE_PORT,
                        prepareSslProperties(memberForMemcacheKeystore, MEMCACHE_PASSWORD)));
        return config;
    }

    protected ServerSocketEndpointConfig createServerSocketConfig(int port, Properties sslProperties) {
        return createServerSocketConfig(port, null, sslProperties);
    }

    protected ServerSocketEndpointConfig createServerSocketConfig(int port, String name, Properties sslProperties) {
        ServerSocketEndpointConfig serverSocketConfig = new ServerSocketEndpointConfig();
        serverSocketConfig.setPort(port);
        serverSocketConfig.getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        if (name != null) {
            serverSocketConfig.setName(name);
        }
        serverSocketConfig.setSSLConfig(new SSLConfig().setEnabled(true).setProperties(sslProperties));
        return serverSocketConfig;
    }

    private RestServerEndpointConfig createRestServerSocketConfig(int port, String name, Properties sslProperties) {
        RestServerEndpointConfig serverSocketConfig = new RestServerEndpointConfig();
        serverSocketConfig.setPort(port);
        serverSocketConfig.getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        if (name != null) {
            serverSocketConfig.setName(name);
        }
        serverSocketConfig.enableAllGroups();
        serverSocketConfig.setSSLConfig(new SSLConfig().setEnabled(true).setProperties(sslProperties));
        return serverSocketConfig;
    }
}
