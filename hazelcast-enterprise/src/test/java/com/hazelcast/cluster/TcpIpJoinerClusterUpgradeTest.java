package com.hazelcast.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Creates a cluster using TCP/IP joiner, then change cluster version.
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({NightlyTest.class})
public class TcpIpJoinerClusterUpgradeTest extends MulticastJoinerClusterUpgradeTest {

    @Override
    protected Config getConfig() {
        Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig()
                .setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig()
                .setEnabled(true)
                .addMember("127.0.0.1");
        return config;
    }
}
