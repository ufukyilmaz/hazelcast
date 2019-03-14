package com.hazelcast.nio.ssl.advancednetwork;

import com.hazelcast.config.Config;
import com.hazelcast.config.ConsistencyCheckStrategy;
import com.hazelcast.config.EndpointConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.ServerSocketEndpointConfig;
import com.hazelcast.config.WANQueueFullBehavior;
import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ssl.TestKeyStoreUtil;
import java.io.File;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import static com.hazelcast.nio.ssl.TestKeyStoreUtil.JAVAX_NET_SSL_KEY_STORE;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.JAVAX_NET_SSL_KEY_STORE_PASSWORD;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.JAVAX_NET_SSL_MUTUAL_AUTHENTICATION;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.JAVAX_NET_SSL_TRUST_STORE;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.JAVAX_NET_SSL_TRUST_STORE_PASSWORD;
import static com.hazelcast.test.HazelcastTestSupport.assertEqualsEventually;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static org.junit.Assert.assertNull;

public abstract class AbstractSecureWanTest {

    protected static final int MEMBER_A_PORT = 5701;
    protected static final int MEMBER_B_PORT = 5702;
    protected static final int MEMBER_C_PORT = 5703;

    private static final String KEYSTORE_AB = "com/hazelcast/nio/ssl/advancednetwork/wanReplicationAB.keystore";
    private static final String TRUSTSTORE_AB = "com/hazelcast/nio/ssl/advancednetwork/wanReplicationAB.truststore";
    private static final String KEYSTORE_BA = "com/hazelcast/nio/ssl/advancednetwork/wanReplicationBA.keystore";
    private static final String TRUSTSTORE_BA = "com/hazelcast/nio/ssl/advancednetwork/wanReplicationBA.truststore";
    private static final String KEYSTORE_BC = "com/hazelcast/nio/ssl/advancednetwork/wanReplicationBC.keystore";
    private static final String TRUSTSTORE_BC = "com/hazelcast/nio/ssl/advancednetwork/wanReplicationBC.truststore";
    private static final String KEYSTORE_CB = "com/hazelcast/nio/ssl/advancednetwork/wanReplicationCB.keystore";
    private static final String TRUSTSTORE_CB = "com/hazelcast/nio/ssl/advancednetwork/wanReplicationCB.truststore";

    protected static final String PASSWORD_AB = "passwordAB";
    protected static final String PASSWORD_BC = "passwordBC";

    protected static final String CLUSTER_A = "ClusterA";
    protected static final String CLUSTER_B = "ClusterB";
    protected static final String CLUSTER_C = "ClusterC";
    protected static final Properties NO_SSL_PROPERTIES = null;

    private static final String NOT_REPLICATED_MAP = "notReplicatedMap";

    protected static File keystoreAB;
    protected static File truststoreAB;
    protected static File keystoreBA;
    protected static File truststoreBA;
    protected static File keystoreBC;
    protected static File truststoreBC;
    protected static File keystoreCB;
    protected static File truststoreCB;

    protected static HazelcastInstance hzA;
    protected static HazelcastInstance hzB;
    protected static HazelcastInstance hzC;

    @BeforeClass
    public static void prepareKeystores() {
        keystoreAB = TestKeyStoreUtil.createTempFile(KEYSTORE_AB);
        truststoreAB = TestKeyStoreUtil.createTempFile(TRUSTSTORE_AB);
        keystoreBA = TestKeyStoreUtil.createTempFile(KEYSTORE_BA);
        truststoreBA = TestKeyStoreUtil.createTempFile(TRUSTSTORE_BA);
        keystoreBC = TestKeyStoreUtil.createTempFile(KEYSTORE_BC);
        truststoreBC = TestKeyStoreUtil.createTempFile(TRUSTSTORE_BC);
        keystoreCB = TestKeyStoreUtil.createTempFile(KEYSTORE_CB);
        truststoreCB = TestKeyStoreUtil.createTempFile(TRUSTSTORE_CB);
    }

    @AfterClass
    public static void removeKeystores() {
        IOUtil.deleteQuietly(keystoreAB);
        IOUtil.deleteQuietly(truststoreAB);
        IOUtil.deleteQuietly(keystoreBA);
        IOUtil.deleteQuietly(truststoreBA);
        IOUtil.deleteQuietly(keystoreBC);
        IOUtil.deleteQuietly(truststoreBC);
        IOUtil.deleteQuietly(keystoreCB);
        IOUtil.deleteQuietly(truststoreCB);
    }

    @After
    public void tearDown() {
        if (hzA != null) {
            hzA.getLifecycleService().terminate();
        }
        if (hzB != null) {
            hzB.getLifecycleService().terminate();
        }
        if (hzC != null) {
            hzC.getLifecycleService().terminate();
        }
    }

    protected void testSuccessfulReplication(HazelcastInstance replicateFrom, final HazelcastInstance replicateTo1,
            final HazelcastInstance replicateTo2, String replicatedMap) {
        testSuccessfulReplication(replicateFrom, replicateTo1, replicateTo2, replicatedMap, replicatedMap);
    }

    protected void testSuccessfulReplication(HazelcastInstance replicateFrom, final HazelcastInstance replicateTo1,
            final HazelcastInstance replicateTo2, final String replicatedMapName1, final String replicatedMapName2) {
        IMap<String, String> replicatedMap = replicateFrom.getMap(replicatedMapName1);
        replicatedMap.put("someKeyIn" + replicatedMapName1, "someValueIn" + replicatedMapName1);
        if (!replicatedMapName1.equals(replicatedMapName2)) {
            IMap<String, String> replicatedMap2 = replicateFrom.getMap(replicatedMapName2);
            replicatedMap2.put("someKeyIn" + replicatedMapName2, "someValueIn" + replicatedMapName2);
        }
        IMap<String, String> notReplicatedMap = replicateFrom.getMap(NOT_REPLICATED_MAP);
        notReplicatedMap.put("someKey", "someValue");

        assertEqualsEventually(new Callable<String>() {
            @Override
            public String call() {
                IMap<String, String> map = replicateTo1.getMap(replicatedMapName1);
                return map.get("someKeyIn" + replicatedMapName1);
            }
        }, "someValueIn" + replicatedMapName1);

        assertEqualsEventually(new Callable<String>() {
            @Override
            public String call() {
                IMap<String, String> map = replicateTo2.getMap(replicatedMapName2);
                return map.get("someKeyIn" + replicatedMapName2);
            }
        }, "someValueIn" + replicatedMapName2);

        IMap<String, String> notReplicatedMap1 = replicateTo1.getMap(NOT_REPLICATED_MAP);
        assertNull(notReplicatedMap1.get("someKey"));
        IMap<String, String> notReplicatedMap2 = replicateTo2.getMap(NOT_REPLICATED_MAP);
        assertNull(notReplicatedMap2.get("someKey"));
    }

    protected Config prepareConfig(String clusterName, int memberPort) {
        Config config = smallInstanceConfig();
        config.getGroupConfig().setName(clusterName);
        JoinConfig join = config.getAdvancedNetworkConfig().getJoin();
        join.getTcpIpConfig().setEnabled(true);
        join.getMulticastConfig().setEnabled(false);
        config.getAdvancedNetworkConfig().setEnabled(true)
                .setMemberEndpointConfig(createMemberSocketConfig(memberPort));
        return config;
    }

    private ServerSocketEndpointConfig createMemberSocketConfig(int port) {
        ServerSocketEndpointConfig serverSocketConfig = new ServerSocketEndpointConfig();
        serverSocketConfig.setPort(port);
        serverSocketConfig.getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        return serverSocketConfig;
    }

    protected ServerSocketEndpointConfig createWanServerSocketConfig(int port, String name, Properties sslProperties) {
        ServerSocketEndpointConfig serverSocketConfig = new ServerSocketEndpointConfig();
        serverSocketConfig.setPort(port);
        serverSocketConfig.getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        serverSocketConfig.setName(name);
        if (sslProperties != null) {
            serverSocketConfig.setSSLConfig(new SSLConfig().setEnabled(true).setProperties(sslProperties));
        }
        return serverSocketConfig;
    }

    protected EndpointConfig createWanEndpointConfig(String name, Properties sslProperties) {
        EndpointConfig endpointConfig = new EndpointConfig();
        endpointConfig.setName(name);
        endpointConfig.getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        if (sslProperties != null) {
            endpointConfig.setSSLConfig(new SSLConfig().setEnabled(true).setProperties(sslProperties));
        }
        return endpointConfig;
    }

    protected Properties prepareSslProperties(File keystore, String keystorePassword, File truststore,
            String truststorePassword) {
        Properties props = new Properties();
        props.setProperty(JAVAX_NET_SSL_KEY_STORE, keystore.getAbsolutePath());
        props.setProperty(JAVAX_NET_SSL_KEY_STORE_PASSWORD, keystorePassword);
        props.setProperty(JAVAX_NET_SSL_TRUST_STORE, truststore.getAbsolutePath());
        props.setProperty(JAVAX_NET_SSL_TRUST_STORE_PASSWORD, truststorePassword);
        props.setProperty(JAVAX_NET_SSL_MUTUAL_AUTHENTICATION, "REQUIRED");
        return props;
    }

    protected static void addCommonWanReplication(Config config, String mapName, String groupName, int port,
            String endpointName) {
        WanReplicationConfig wrConfig = new WanReplicationConfig();
        wrConfig.setName("wan-cluster-for-" + mapName);
        WanPublisherConfig londonPublisherConfig = createWanPublisherConfig(
                groupName,
                "127.0.0.1:" + port,
                endpointName
        );
        wrConfig.addWanPublisherConfig(londonPublisherConfig);

        config.addWanReplicationConfig(wrConfig);

        WanReplicationRef wanRef = new WanReplicationRef();
        wanRef.setName("wan-cluster-for-" + mapName);
        wanRef.setMergePolicy(PassThroughMergePolicy.class.getName());
        wanRef.setRepublishingEnabled(true);

        config.getMapConfig(mapName).setWanReplicationRef(wanRef);
    }

    private static WanPublisherConfig createWanPublisherConfig(String clusterName, String endpoints, String endpointName) {
        WanPublisherConfig publisherConfig = new WanPublisherConfig();
        publisherConfig.setEndpoint(endpointName);
        publisherConfig.setGroupName(clusterName);
        publisherConfig.setClassName("com.hazelcast.enterprise.wan.replication.WanBatchReplication");
        publisherConfig.setQueueFullBehavior(WANQueueFullBehavior.DISCARD_AFTER_MUTATION);
        publisherConfig.setQueueCapacity(1000);
        publisherConfig.getWanSyncConfig().setConsistencyCheckStrategy(ConsistencyCheckStrategy.NONE);
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
}
