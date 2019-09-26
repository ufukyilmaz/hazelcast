package com.hazelcast.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.version.MemberVersion;
import org.junit.After;

import static com.hazelcast.instance.BuildInfoProvider.HAZELCAST_INTERNAL_OVERRIDE_VERSION;

/**
 * Abstract class for split-brain cluster upgrade tests.
 */
public abstract class AbstractSplitBrainUpgradeTest extends SplitBrainTestSupport {

    private String clusterName;

    @Override
    protected void onBeforeSetup() {
        System.setProperty("hazelcast.max.join.seconds", "10");
        clusterName = randomName();
    }

    @Override
    protected boolean shouldAssertAllNodesRejoined() {
        // sleep for a while to allow split-brain handler to execute
        sleepSeconds(30);
        return false;
    }

    @After
    public void teardown() {
        System.clearProperty("hazelcast.max.join.seconds");
    }

    @Override
    protected Config config() {
        Config config = super.config();
        config.setClusterName(clusterName);
        return config;
    }

    @SuppressWarnings("SameParameterValue")
    HazelcastInstance createHazelcastInstanceInBrain(int brain, MemberVersion memberVersion) {
        String existingValue = System.getProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION);
        System.setProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION, memberVersion.toString());
        try {
            return createHazelcastInstanceInBrain(brain);
        } finally {
            if (existingValue != null) {
                System.setProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION, existingValue);
            } else {
                System.clearProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION);
            }
        }
    }
}
