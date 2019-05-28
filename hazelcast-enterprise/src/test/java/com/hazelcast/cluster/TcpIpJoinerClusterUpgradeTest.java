package com.hazelcast.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.SerializationSamplesExcluded;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

/**
 * Creates a cluster using TCP/IP joiner, then change cluster version.
 */
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({NightlyTest.class, SerializationSamplesExcluded.class})
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
