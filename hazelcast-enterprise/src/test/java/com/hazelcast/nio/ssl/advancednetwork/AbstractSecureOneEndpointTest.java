package com.hazelcast.nio.ssl.advancednetwork;

import com.hazelcast.config.Config;
import com.hazelcast.config.ConsistencyCheckStrategy;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.RestServerEndpointConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.ServerSocketEndpointConfig;
import com.hazelcast.config.WANQueueFullBehavior;
import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.internal.ascii.HTTPCommunicator;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.spi.properties.GroupProperty;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.FailureMode;
import net.spy.memcached.MemcachedClient;
import org.junit.Before;
import org.junit.Test;

import static com.hazelcast.test.HazelcastTestSupport.assertEqualsEventually;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static org.junit.Assert.assertEquals;

public abstract class AbstractSecureOneEndpointTest extends AbstractSecuredEndpointsTest {

    protected static final String REPLICATED_MAP = "replicatedMap";

    @Before
    public void setup() {
        Config config = createCompleteMultiSocketConfigWithSecurity();
        configureTcpIpConfig(config);
        hz = Hazelcast.newHazelcastInstance(config);
    }

    @Test
    public void testMemberConnectionToEndpoints() {
        Config config = smallInstanceConfig();
        config.getAdvancedNetworkConfig().setEnabled(true);
        JoinConfig join = config.getAdvancedNetworkConfig().getJoin();
        join.getTcpIpConfig().addMember("127.0.0.1:" + MEMBER_PORT).setEnabled(true);
        join.getMulticastConfig().setEnabled(false);
        config.setProperty(GroupProperty.MAX_JOIN_SECONDS.getName(), "3");
        HazelcastInstance newHzInstance = null;
        try {
            newHzInstance = Hazelcast.newHazelcastInstance(config);
            int clusterSize = newHzInstance.getCluster().getMembers().size();
            assertEquals(2, clusterSize);
        } finally {
            if (newHzInstance != null) {
                newHzInstance.shutdown();
            }
        }
    }

    @Test
    public void testWanConnectionToEndpoints() {
        Config config2 = prepareWanAdvancedNetworkConfig(WAN_PORT, null);
        HazelcastInstance hz2 = null;
        try {
            hz2 = Hazelcast.newHazelcastInstance(config2);
            IMap<String, String> map = hz2.getMap(REPLICATED_MAP);
            map.put("someKey", "someValue");

            assertEqualsEventually(new Callable<String>() {
                @Override
                public String call() {
                    IMap<String, String> map1 = hz.getMap(REPLICATED_MAP);
                    return map1.get("someKey");
                }
            }, "someValue");
        } finally {
            if (hz2 != null) {
                hz2.shutdown();
            }
        }
    }

    @Test
    public void testRestConnectionToEndpoints() throws Exception {
        HTTPCommunicator communicator = new HTTPCommunicator(hz, "/127.0.0.1:" + REST_PORT);
        final String expected = "{\"status\":\"success\","
                + "\"version\":\"" + hz.getCluster().getClusterVersion().toString() + "\"}";
        assertEquals(expected, communicator.getClusterVersion());
    }

    @Test
    public void testMemcacheConnectionToEndpoints() throws Exception {
        MemcachedClient client = null;
        try {
            client = getMemcachedClient(hz, MEMCACHE_PORT);
            client.get("whatever");
        } finally {
            if (client != null) {
                client.shutdown();
            }
        }
    }

    private Config createCompleteMultiSocketConfigWithSecurity() {
        Config config = smallInstanceConfig();
        config.getAdvancedNetworkConfig().setEnabled(true)
                .setMemberEndpointConfig(createServerSocketConfig(MEMBER_PORT, prepareMemberEndpointSsl()))
                .setClientEndpointConfig(createServerSocketConfig(CLIENT_PORT, prepareClientEndpointSsl()))
                .addWanEndpointConfig(createServerSocketConfig(WAN_PORT, "WAN", prepareWanEndpointSsl()))
                .setRestEndpointConfig(createRestServerSocketConfig(REST_PORT, "REST", prepareRestEndpointSsl()))
                .setMemcacheEndpointConfig(createServerSocketConfig(MEMCACHE_PORT, prepareMemcacheEndpointSsl()));
        return config;
    }

    protected Properties prepareMemberEndpointSsl() {
        return null;
    }

    protected Properties prepareClientEndpointSsl() {
        return null;
    }

    protected Properties prepareWanEndpointSsl() {
        return null;
    }

    protected Properties prepareRestEndpointSsl() {
        return null;
    }

    protected Properties prepareMemcacheEndpointSsl() {
        return null;
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
        if (sslProperties != null) {
            serverSocketConfig.setSSLConfig(new SSLConfig().setEnabled(true).setProperties(sslProperties));
        }
        return serverSocketConfig;
    }

    protected RestServerEndpointConfig createRestServerSocketConfig(int port, String name, Properties sslProperties) {
        RestServerEndpointConfig serverSocketConfig = new RestServerEndpointConfig();
        serverSocketConfig.setPort(port);
        serverSocketConfig.getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        if (name != null) {
            serverSocketConfig.setName(name);
        }
        serverSocketConfig.enableAllGroups();
        if (sslProperties != null) {
            serverSocketConfig.setSSLConfig(new SSLConfig().setEnabled(true).setProperties(sslProperties));
        }
        return serverSocketConfig;
    }

    protected Config prepareWanAdvancedNetworkConfig(int port, Properties sslProperties) {
        Config config = smallInstanceConfig();
        config.getAdvancedNetworkConfig()
                .setEnabled(true)
                .addWanEndpointConfig(createServerSocketConfig(WAN_PORT + 10, "WAN", sslProperties));
        addCommonWanReplication(config, port);
        config.getGroupConfig().setName("not-dev-cluster");
        return config;
    }

    private static void addCommonWanReplication(Config config, int port) {
        WanReplicationConfig wrConfig = new WanReplicationConfig();
        wrConfig.setName("my-wan-cluster");
        WanPublisherConfig londonPublisherConfig = createWanPublisherConfig(
                "dev",
                "127.0.0.1:" + port,
                ConsistencyCheckStrategy.NONE
        );
        wrConfig.addWanPublisherConfig(londonPublisherConfig);

        config.addWanReplicationConfig(wrConfig);

        WanReplicationRef wanRef = new WanReplicationRef();
        wanRef.setName("my-wan-cluster");
        wanRef.setMergePolicy(PassThroughMergePolicy.class.getName());
        wanRef.setRepublishingEnabled(false);

        config.getMapConfig(REPLICATED_MAP).setWanReplicationRef(wanRef);
    }

    private static WanPublisherConfig createWanPublisherConfig(String clusterName, String endpoints,
            ConsistencyCheckStrategy consistencyStrategy) {
        WanPublisherConfig publisherConfig = new WanPublisherConfig();
        publisherConfig.setEndpoint("WAN");
        publisherConfig.setGroupName(clusterName);
        publisherConfig.setClassName("com.hazelcast.enterprise.wan.impl.replication.WanBatchReplication");
        publisherConfig.setQueueFullBehavior(WANQueueFullBehavior.DISCARD_AFTER_MUTATION);
        publisherConfig.setQueueCapacity(1000);
        publisherConfig.getWanSyncConfig().setConsistencyCheckStrategy(consistencyStrategy);
        Map<String, Comparable> props = publisherConfig.getProperties();
        props.put("batch.size", 500);
        props.put("batch.max.delay.millis", 1000);
        props.put("snapshot.enabled", false);
        props.put("response.timeout.millis", 60000);
        props.put("ack.type", WanAcknowledgeType.ACK_ON_OPERATION_COMPLETE.toString());
        props.put("endpoints", endpoints);
        props.put("discovery.period", "20");
        props.put("executorThreadCount", "2");
        return publisherConfig;
    }

    private MemcachedClient getMemcachedClient(HazelcastInstance instance, int port) throws Exception {
        String hostName = instance.getCluster().getLocalMember().getSocketAddress().getHostName();
        InetSocketAddress address = new InetSocketAddress(hostName, port);
        ConnectionFactory factory = new ConnectionFactoryBuilder()
                .setOpTimeout(3000)
                .setDaemon(true)
                .setFailureMode(FailureMode.Retry)
                .build();
        return new MemcachedClient(factory, Collections.singletonList(address));
    }
}
