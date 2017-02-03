package com.hazelcast.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Create a cluster using TCP/IP joiner, then change cluster version.
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({NightlyTest.class})
public class TcpipJoinerClusterUpgradeTest extends MulticastJoinerClusterUpgradeTest {

    @Override
    protected Config getConfig() {
        Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        config.getNetworkConfig().getJoin().getTcpIpConfig().addMember("127.0.0.1");
        return config;
    }
}
