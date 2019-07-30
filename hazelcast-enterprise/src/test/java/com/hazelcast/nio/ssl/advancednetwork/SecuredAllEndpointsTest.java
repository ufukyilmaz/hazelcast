package com.hazelcast.nio.ssl.advancednetwork;

import com.hazelcast.config.Config;
import com.hazelcast.config.ConsistencyCheckStrategy;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.WANQueueFullBehavior;
import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.config.WanBatchReplicationPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.Properties;

import static com.hazelcast.nio.ssl.TestKeyStoreUtil.JAVAX_NET_SSL_TRUST_STORE;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.JAVAX_NET_SSL_TRUST_STORE_PASSWORD;
import static com.hazelcast.test.HazelcastTestSupport.assertEqualsEventually;
import static com.hazelcast.test.HazelcastTestSupport.sleepSeconds;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(SlowTest.class)
public class SecuredAllEndpointsTest extends AbstractSecuredAllEndpointsTest {

    private static final String REPLICATED_MAP = "replicatedMap";

    // When using IBM JSSE2 and getting the SSLContext with "TLS" as protocol
    // JSSE2 will only enable usage of TLS V1.0 even though other protocols are
    // supported. To bring it in line with the Oracle implementation and allow
    // TLS V1.1 and TLS V1.2, we need to set this property.
    // see:
    // https://github.com/hazelcast/hazelcast-enterprise/issues/2824
    // https://www.ibm.com/support/knowledgecenter/en/SSYKE2_7.1.0/com.ibm.java.security.component.71.doc/security-component/jsse2Docs/matchsslcontext_tls.html#matchsslcontext_tls
    @Rule
    public OverridePropertyRule rule = OverridePropertyRule.set("com.ibm.jsse2.overrideDefaultTLS", "true");

    @Test
    public void testMemberEndpointWithMemberCertificate() {
        Config config = configForTestMemberEndpoint(memberBKeystore, MEMBER_PASSWORD, memberBTruststore,
                MEMBER_PASSWORD);
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
    public void testMemberEndpointWithClientCertificate() {
        testMemberEndpointWithIncorrectCertificate(memberBKeystore, MEMBER_PASSWORD, clientTruststore, CLIENT_PASSWORD);
    }

    @Test
    public void testMemberEndpointWithWanCertificate() {
        testMemberEndpointWithIncorrectCertificate(memberBKeystore, MEMBER_PASSWORD, wanTruststore, WAN_PASSWORD);
    }

    @Test
    public void testMemberEndpointWithRestCertificate() {
        testMemberEndpointWithIncorrectCertificate(memberBKeystore, MEMBER_PASSWORD, restTruststore, REST_PASSWORD);
    }

    @Test
    public void testMemberEndpointWithMemcacheCertificate() {
        testMemberEndpointWithIncorrectCertificate(memberBKeystore, MEMBER_PASSWORD, memcacheTruststore, MEMCACHE_PASSWORD);
    }

    @Test
    public void testWanEndpointWithWanCertificate() {
        Config config2 = prepareWanAdvancedNetworkConfig(wanTruststore, WAN_PASSWORD);
        HazelcastInstance hz2 = null;
        try {
            hz2 = Hazelcast.newHazelcastInstance(config2);
            IMap<String, String> map = hz2.getMap(REPLICATED_MAP);
            map.put("someKey", "someValue");

            assertEqualsEventually(() -> {
                IMap<String, String> map1 = hz.getMap(REPLICATED_MAP);
                return map1.get("someKey");
            }, "someValue");
        } finally {
            if (hz2 != null) {
                hz2.shutdown();
            }
        }
    }

    @Test
    public void testWanEndpointWithMemberCertificate() {
        testWanEndpointWithIncorrectCertificate(memberBTruststore, MEMBER_PASSWORD);
    }

    @Test
    public void testWanEndpointWithClientCertificate() {
        testWanEndpointWithIncorrectCertificate(clientTruststore, CLIENT_PASSWORD);
    }

    @Test
    public void testWanEndpointWithRestCertificate() {
        testWanEndpointWithIncorrectCertificate(restTruststore, REST_PASSWORD);
    }

    @Test
    public void testWanEndpointWithMemcacheCertificate() {
        testWanEndpointWithIncorrectCertificate(memcacheTruststore, MEMCACHE_PASSWORD);
    }

    @Test
    public void testRestEndpointWithRestCertificate() throws Exception {
        testTextEndpoint(REST_PORT, restTruststore, REST_PASSWORD, true);
    }

    @Test
    public void testRestEndpointWithUntrustedCertificate() throws Exception {
        testTextEndpoint(REST_PORT, clientTruststore, CLIENT_PASSWORD, false);
    }

    @Test
    public void testMemcacheEndpointWithMemcacheCertificate() throws Exception {
        testTextEndpoint(MEMCACHE_PORT, memcacheTruststore, MEMCACHE_PASSWORD, true);
    }

    @Test
    public void testMemcacheEndpointWithUntrustedCertificate() throws Exception {
        testTextEndpoint(MEMCACHE_PORT, clientTruststore, CLIENT_PASSWORD, false);
    }

    private void testMemberEndpointWithIncorrectCertificate(File keystore, String keystorePassword, File truststore,
                                                            String truststorePassword) {
        Config config = configForTestMemberEndpoint(keystore, keystorePassword, truststore, truststorePassword);
        HazelcastInstance newHzInstance = null;
        try {
            newHzInstance = Hazelcast.newHazelcastInstance(config);
            fail("It should fail because certificate should not be trusted");
        } catch (IllegalStateException ex) {
            // expected
        } finally {
            if (newHzInstance != null) {
                newHzInstance.shutdown();
            }
        }
    }

    private Config configForTestMemberEndpoint(File keystore, String keystorePassword, File truststore,
                                               String truststorePassword) {
        Config config = smallInstanceConfig();
        config.getAdvancedNetworkConfig().setEnabled(true)
                .setMemberEndpointConfig(createServerSocketConfig(MEMBER_PORT + 10,
                        prepareSslPropertiesWithTrustStore(keystore, keystorePassword, truststore, truststorePassword)));
        JoinConfig join = config.getAdvancedNetworkConfig().getJoin();
        join.getTcpIpConfig().addMember("127.0.0.1:" + MEMBER_PORT).setEnabled(true);
        join.getMulticastConfig().setEnabled(false);
        config.setProperty(GroupProperty.MAX_JOIN_SECONDS.getName(), "3");
        return config;
    }

    private Config prepareWanAdvancedNetworkConfig(File truststore, String truststorePassword) {
        Properties props = new Properties();
        props.setProperty(JAVAX_NET_SSL_TRUST_STORE, truststore.getAbsolutePath());
        props.setProperty(JAVAX_NET_SSL_TRUST_STORE_PASSWORD, truststorePassword);
        Config config = smallInstanceConfig();
        config.getAdvancedNetworkConfig()
                .setEnabled(true)
                .addWanEndpointConfig(createServerSocketConfig(WAN_PORT + 10, "WAN", props));
        addCommonWanReplication(config, WAN_PORT);
        config.getGroupConfig().setName("not-dev-cluster");
        return config;
    }

    private void testWanEndpointWithIncorrectCertificate(File truststore, String truststorePassword) {
        Config config = prepareWanAdvancedNetworkConfig(truststore, truststorePassword);
        HazelcastInstance hz2 = null;
        try {
            hz2 = Hazelcast.newHazelcastInstance(config);
            IMap<String, String> map = hz2.getMap(REPLICATED_MAP);
            map.put("keyWhichIsNotReplicated", "someValueWhichIsNotReplicated");

            // we have to sleep here some time since we basically test that nothing was changed
            sleepSeconds(3);

            IMap<String, String> map1 = hz.getMap(REPLICATED_MAP);
            assertNull(map1.get("keyWhichIsNotReplicated"));
        } finally {
            if (hz2 != null) {
                hz2.shutdown();
            }
        }
    }

    private static void addCommonWanReplication(Config config, int port) {
        WanReplicationConfig wrConfig = new WanReplicationConfig();
        wrConfig.setName("my-wan-cluster");
        WanBatchReplicationPublisherConfig pc = createWanPublisherConfig(
                "dev",
                "127.0.0.1:" + port,
                ConsistencyCheckStrategy.NONE
        );
        wrConfig.addWanBatchReplicationPublisherConfig(pc);

        config.addWanReplicationConfig(wrConfig);

        WanReplicationRef wanRef = new WanReplicationRef();
        wanRef.setName("my-wan-cluster");
        wanRef.setMergePolicy(PassThroughMergePolicy.class.getName());
        wanRef.setRepublishingEnabled(false);

        config.getMapConfig(REPLICATED_MAP).setWanReplicationRef(wanRef);
    }

    private static WanBatchReplicationPublisherConfig createWanPublisherConfig(String clusterName,
                                                                               String endpoints,
                                                                               ConsistencyCheckStrategy consistencyStrategy) {
        WanBatchReplicationPublisherConfig pc = new WanBatchReplicationPublisherConfig()
                .setEndpoint("WAN")
                .setGroupName(clusterName)
                .setQueueFullBehavior(WANQueueFullBehavior.DISCARD_AFTER_MUTATION)
                .setQueueCapacity(1000)
                .setBatchSize(500)
                .setBatchMaxDelayMillis(1000)
                .setSnapshotEnabled(false)
                .setResponseTimeoutMillis(60000)
                .setAcknowledgeType(WanAcknowledgeType.ACK_ON_OPERATION_COMPLETE)
                .setTargetEndpoints(endpoints)
                .setDiscoveryPeriodSeconds(20);
        pc.getWanSyncConfig().setConsistencyCheckStrategy(consistencyStrategy);
        return pc;
    }

}
