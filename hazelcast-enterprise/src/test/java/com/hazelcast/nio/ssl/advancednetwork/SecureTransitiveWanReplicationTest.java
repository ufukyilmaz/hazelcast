package com.hazelcast.nio.ssl.advancednetwork;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import java.util.Properties;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Following WAN replication is used in whole this test: ClusterA -> ClusterB -> ClusterC.
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(SlowTest.class)
public class SecureTransitiveWanReplicationTest extends AbstractSecureWanTest {

    private static final String REPLICATED_MAP = "replicatedMap";
    private static final String WAN_AB_REPLICATION = "wan-a-b-replication";
    private static final String WAN_BC_REPLICATION = "wan-b-c-replication";

    private static final int WAN_B_PORT = 11000;
    private static final int WAN_C_PORT = 11001;

    @Test
    public void testNonSecureReplication() {
        startClusterA(NO_SSL_PROPERTIES);
        startClusterB(NO_SSL_PROPERTIES, NO_SSL_PROPERTIES);
        startClusterC(NO_SSL_PROPERTIES);
        testSuccessfulReplication();
    }

    @Test
    public void testSecureFirstReplication() {
        startClusterA(prepareSslProperties(keystoreAB, PASSWORD_AB, truststoreAB, PASSWORD_AB));
        startClusterB(prepareSslProperties(keystoreBA, PASSWORD_AB, truststoreBA, PASSWORD_AB), NO_SSL_PROPERTIES);
        startClusterC(NO_SSL_PROPERTIES);
        testSuccessfulReplication();
    }

    @Test
    public void testSecureSecondReplication() {
        startClusterA(NO_SSL_PROPERTIES);
        startClusterB(NO_SSL_PROPERTIES, prepareSslProperties(keystoreBC, PASSWORD_BC, truststoreBC, PASSWORD_BC));
        startClusterC(prepareSslProperties(keystoreCB, PASSWORD_BC, truststoreCB, PASSWORD_BC));
        testSuccessfulReplication();
    }

    @Test
    public void testSecureBothReplications() {
        startClusterA(prepareSslProperties(keystoreAB, PASSWORD_AB, truststoreAB, PASSWORD_AB));
        startClusterB(prepareSslProperties(keystoreBA, PASSWORD_AB, truststoreBA, PASSWORD_AB),
                prepareSslProperties(keystoreBC, PASSWORD_BC, truststoreBC, PASSWORD_BC));
        startClusterC(prepareSslProperties(keystoreCB, PASSWORD_BC, truststoreCB, PASSWORD_BC));
        testSuccessfulReplication();
    }

    private void testSuccessfulReplication() {
        testSuccessfulReplication(hzA, hzB, hzC, REPLICATED_MAP);
    }

    private void startClusterA(Properties sslProperties) {
        Config configA = prepareConfig(CLUSTER_A, MEMBER_A_PORT);
        configA.getAdvancedNetworkConfig().addWanEndpointConfig(
                createWanEndpointConfig(WAN_AB_REPLICATION, sslProperties));
        addCommonWanReplication(configA, REPLICATED_MAP, CLUSTER_B, WAN_B_PORT, WAN_AB_REPLICATION);
        hzA = Hazelcast.newHazelcastInstance(configA);
    }

    private void startClusterB(Properties inputSslProperties, Properties outputSslProperties) {
        Config configB = prepareConfig(CLUSTER_B, MEMBER_B_PORT);
        configB.getAdvancedNetworkConfig()
                .addWanEndpointConfig(
                        createWanServerSocketConfig(WAN_B_PORT, WAN_AB_REPLICATION, inputSslProperties))
                .addWanEndpointConfig(
                        createWanEndpointConfig(WAN_BC_REPLICATION, outputSslProperties));
        addCommonWanReplication(configB, REPLICATED_MAP, CLUSTER_C, WAN_C_PORT, WAN_BC_REPLICATION);
        hzB = Hazelcast.newHazelcastInstance(configB);
    }

    private void startClusterC(Properties sslProperties) {
        Config configC = prepareConfig(CLUSTER_C, MEMBER_C_PORT);
        configC.getAdvancedNetworkConfig()
                .addWanEndpointConfig(
                        createWanServerSocketConfig(WAN_C_PORT, WAN_BC_REPLICATION, sslProperties));
        hzC = Hazelcast.newHazelcastInstance(configC);
    }
}
