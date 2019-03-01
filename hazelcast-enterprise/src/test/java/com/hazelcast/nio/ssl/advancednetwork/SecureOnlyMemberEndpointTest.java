package com.hazelcast.nio.ssl.advancednetwork;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.annotation.SlowTest;
import java.util.Properties;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.nio.ssl.advancednetwork.AbstractSecuredEndpointsTest.memberBKeystore;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(SlowTest.class)
public class SecureOnlyMemberEndpointTest extends AbstractSecureOneEndpointTest {

    @Test
    @Override
    public void testMemberConnectionToEndpoints() {
        Config config = smallInstanceConfig();
        config.getAdvancedNetworkConfig().setEnabled(true)
                .setMemberEndpointConfig(createServerSocketConfig(MEMBER_PORT + 10,
                        prepareSslPropertiesWithTrustStore(memberBKeystore, MEMBER_PASSWORD, memberBTruststore,
                                MEMBER_PASSWORD)));
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

    @Override
    protected Properties prepareMemberEndpointSsl() {
        return prepareSslPropertiesWithTrustStore(memberAKeystore, MEMBER_PASSWORD, memberATruststore, MEMBER_PASSWORD);
    }
}
