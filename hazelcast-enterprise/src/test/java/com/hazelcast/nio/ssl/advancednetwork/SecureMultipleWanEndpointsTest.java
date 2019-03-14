package com.hazelcast.nio.ssl.advancednetwork;

import com.hazelcast.config.Config;
import com.hazelcast.config.EndpointConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import java.util.Properties;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.nio.ssl.advancednetwork.AbstractSecureWanTest.NO_SSL_PROPERTIES;
import static com.hazelcast.nio.ssl.advancednetwork.AbstractSecureWanTest.hzA;

/**
 * Following two WAN replications are used in whole this test: ClusterB -> ClusterA and ClusterB -> ClusterC.
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(SlowTest.class)
public class SecureMultipleWanEndpointsTest extends AbstractSecureWanTest {

    private static final String REPLICATED_MAP_TO_CLUSTER_A = "replicatedMapToA";
    private static final String REPLICATED_MAP_TO_CLUSTER_C = "replicatedMapToC";
    private static final String WAN_BA_REPLICATION = "wan-b-a-replication";
    private static final String WAN_BC_REPLICATION = "wan-b-c-replication";

    private static final int WAN_A_PORT = 11000;
    private static final int WAN_B_TO_A_PORT = 11001;
    private static final int WAN_B_TO_C_PORT = 11002;
    private static final int WAN_C_PORT = 11003;

    @Test
    public void testNonSecureReplication_bothWanEndpoint() {
        startClusterB_bothWanEndpoint(NO_SSL_PROPERTIES, NO_SSL_PROPERTIES);
        startClusterA(NO_SSL_PROPERTIES);
        startClusterC(NO_SSL_PROPERTIES);
        testSuccessfulReplication();
    }

    @Test
    public void testNonSecureReplication_wanEndpoint_wanServerSocketEndpoint() {
        startClusterB_wanEndpoint_wanServerSocketEndpoint(NO_SSL_PROPERTIES, NO_SSL_PROPERTIES);
        startClusterA(NO_SSL_PROPERTIES);
        startClusterC(NO_SSL_PROPERTIES);
        testSuccessfulReplication();
    }

    @Test
    public void testNonSecureReplication_bothWanServerSocketEndpoint() {
        startClusterB_bothWanServerSocketEndpoint(NO_SSL_PROPERTIES, NO_SSL_PROPERTIES);
        startClusterA(NO_SSL_PROPERTIES);
        startClusterC(NO_SSL_PROPERTIES);
        testSuccessfulReplication();
    }

    @Test
    public void testOneSecureReplication_bothWanEndpoint() {
        startClusterB_bothWanEndpoint(
                prepareSslProperties(keystoreBA, PASSWORD_AB, truststoreBA, PASSWORD_AB), NO_SSL_PROPERTIES);
        startClusterA(prepareSslProperties(keystoreAB, PASSWORD_AB, truststoreAB, PASSWORD_AB));
        startClusterC(NO_SSL_PROPERTIES);
        testSuccessfulReplication();
    }

    @Test
    public void testSecureWanEndpointReplication_wanEndpoint_wanServerSocketEndpoint() {
        startClusterB_wanEndpoint_wanServerSocketEndpoint(
                prepareSslProperties(keystoreBA, PASSWORD_AB, truststoreBA, PASSWORD_AB), NO_SSL_PROPERTIES);
        startClusterA(prepareSslProperties(keystoreAB, PASSWORD_AB, truststoreAB, PASSWORD_AB));
        startClusterC(NO_SSL_PROPERTIES);
        testSuccessfulReplication();
    }

    @Test
    public void testSecureWanServerSocketEndpointReplication_wanEndpoint_wanServerSocketEndpoint() {
        startClusterB_wanEndpoint_wanServerSocketEndpoint(NO_SSL_PROPERTIES,
                prepareSslProperties(keystoreBC, PASSWORD_BC, truststoreBC, PASSWORD_BC));
        startClusterA(NO_SSL_PROPERTIES);
        startClusterC(prepareSslProperties(keystoreCB, PASSWORD_BC, truststoreCB, PASSWORD_BC));
        testSuccessfulReplication();
    }

    @Test
    public void testOneSecureReplication_bothWanServerSocketEndpoint() {
        startClusterB_bothWanServerSocketEndpoint(
                prepareSslProperties(keystoreBA, PASSWORD_AB, truststoreBA, PASSWORD_AB), NO_SSL_PROPERTIES);
        startClusterA(prepareSslProperties(keystoreAB, PASSWORD_AB, truststoreAB, PASSWORD_AB));
        startClusterC(NO_SSL_PROPERTIES);
        testSuccessfulReplication();
    }

    @Test
    public void testSecureBothReplications_bothWanEndpoint() {
        startClusterB_bothWanEndpoint(
                prepareSslProperties(keystoreBA, PASSWORD_AB, truststoreBA, PASSWORD_AB),
                prepareSslProperties(keystoreBC, PASSWORD_BC, truststoreBC, PASSWORD_BC));
        startClusterA(prepareSslProperties(keystoreAB, PASSWORD_AB, truststoreAB, PASSWORD_AB));
        startClusterC(prepareSslProperties(keystoreCB, PASSWORD_BC, truststoreCB, PASSWORD_BC));
        testSuccessfulReplication();
    }

    @Test
    public void testSecureBothReplications_wanEndpoint_wanServerSocketEndpoint() {
        startClusterB_wanEndpoint_wanServerSocketEndpoint(
                prepareSslProperties(keystoreBA, PASSWORD_AB, truststoreBA, PASSWORD_AB),
                prepareSslProperties(keystoreBC, PASSWORD_BC, truststoreBC, PASSWORD_BC));
        startClusterA(prepareSslProperties(keystoreAB, PASSWORD_AB, truststoreAB, PASSWORD_AB));
        startClusterC(prepareSslProperties(keystoreCB, PASSWORD_BC, truststoreCB, PASSWORD_BC));
        testSuccessfulReplication();
    }

    @Test
    public void testSecureBothReplications_bothWanServerSocketEndpoint() {
        startClusterB_bothWanServerSocketEndpoint(
                prepareSslProperties(keystoreBA, PASSWORD_AB, truststoreBA, PASSWORD_AB),
                prepareSslProperties(keystoreBC, PASSWORD_BC, truststoreBC, PASSWORD_BC));
        startClusterA(prepareSslProperties(keystoreAB, PASSWORD_AB, truststoreAB, PASSWORD_AB));
        startClusterC(prepareSslProperties(keystoreCB, PASSWORD_BC, truststoreCB, PASSWORD_BC));
        testSuccessfulReplication();
    }

    private void testSuccessfulReplication() {
        testSuccessfulReplication(hzB, hzA, hzC, REPLICATED_MAP_TO_CLUSTER_A, REPLICATED_MAP_TO_CLUSTER_C);
    }

    private void startClusterB_bothWanEndpoint(Properties sslPropertiesA, Properties sslPropertiesC) {
        startClusterB(
                createWanEndpointConfig(WAN_BA_REPLICATION, sslPropertiesA),
                createWanEndpointConfig(WAN_BC_REPLICATION, sslPropertiesC));
    }

    private void startClusterB_wanEndpoint_wanServerSocketEndpoint(Properties sslPropertiesA, Properties sslPropertiesC) {
        startClusterB(
                createWanEndpointConfig(WAN_BA_REPLICATION, sslPropertiesA),
                createWanServerSocketConfig(WAN_B_TO_C_PORT, WAN_BC_REPLICATION, sslPropertiesC));
    }

    private void startClusterB_bothWanServerSocketEndpoint(Properties sslPropertiesA, Properties sslPropertiesC) {
        startClusterB(
                createWanServerSocketConfig(WAN_B_TO_A_PORT, WAN_BA_REPLICATION, sslPropertiesA),
                createWanServerSocketConfig(WAN_B_TO_C_PORT, WAN_BC_REPLICATION, sslPropertiesC));
    }

    private void startClusterB(EndpointConfig clusterA, EndpointConfig clusterC) {
        Config configB = prepareConfig(CLUSTER_B, MEMBER_B_PORT);
        configB.getAdvancedNetworkConfig()
                .addWanEndpointConfig(clusterA)
                .addWanEndpointConfig(clusterC);
        addCommonWanReplication(configB, REPLICATED_MAP_TO_CLUSTER_A, CLUSTER_A, WAN_A_PORT, WAN_BA_REPLICATION);
        addCommonWanReplication(configB, REPLICATED_MAP_TO_CLUSTER_C, CLUSTER_C, WAN_C_PORT, WAN_BC_REPLICATION);
        hzB = Hazelcast.newHazelcastInstance(configB);
    }

    private void startClusterA(Properties sslProperties) {
        Config configA = prepareConfig(CLUSTER_A, MEMBER_A_PORT);
        configA.getAdvancedNetworkConfig()
                .addWanEndpointConfig(
                        createWanServerSocketConfig(WAN_A_PORT, WAN_BA_REPLICATION, sslProperties));
        hzA = Hazelcast.newHazelcastInstance(configA);
    }

    private void startClusterC(Properties sslProperties) {
        Config configC = prepareConfig(CLUSTER_C, MEMBER_C_PORT);
        configC.getAdvancedNetworkConfig()
                .addWanEndpointConfig(
                        createWanServerSocketConfig(WAN_C_PORT, WAN_BC_REPLICATION, sslProperties));
        hzC = Hazelcast.newHazelcastInstance(configC);
    }
}
