package com.hazelcast.internal.management;

import com.hazelcast.config.Config;
import com.hazelcast.config.HotRestartClusterDataRecoveryPolicy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.hotrestart.HotRestartFolderRule;
import com.hazelcast.internal.hotrestart.InternalHotRestartService;
import com.hazelcast.internal.management.dto.ClusterHotRestartStatusDTO;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.Rule;

import static org.junit.Assert.assertFalse;

public abstract class HotRestartConsoleTestSupport extends HazelcastTestSupport {

    @Rule
    public HotRestartFolderRule hotRestartFolderRule = new HotRestartFolderRule();

    protected TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

    protected void shutdown(final HazelcastInstance hz1, HazelcastInstance hz2) {
        hz2.getCluster().shutdown();
        assertTrueEventually(() -> assertFalse(hz1.getLifecycleService().isRunning()));
    }

    protected Config newConfig() {
        Config config = smallInstanceConfig();
        config.getHotRestartPersistenceConfig().setEnabled(true).setBaseDir(hotRestartFolderRule.getBaseDir())
                .setClusterDataRecoveryPolicy(HotRestartClusterDataRecoveryPolicy.PARTIAL_RECOVERY_MOST_COMPLETE);
        return config;
    }

    public static ClusterHotRestartStatusDTO getClusterHotRestartStatus(HazelcastInstance instance) {
        InternalHotRestartService internalHotRestartService = getNode(instance).getNodeExtension().getInternalHotRestartService();
        return internalHotRestartService.getCurrentClusterHotRestartStatus();
    }
}
