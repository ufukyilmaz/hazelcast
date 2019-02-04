package com.hazelcast.internal.management.request;

import com.hazelcast.config.Config;
import com.hazelcast.config.HotRestartClusterDataRecoveryPolicy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.hotrestart.InternalHotRestartService;
import com.hazelcast.internal.management.dto.ClusterHotRestartStatusDTO;
import com.hazelcast.spi.hotrestart.HotRestartFolderRule;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.Rule;

import static org.junit.Assert.assertFalse;

public abstract class HotRestartConsoleRequestTestSupport extends HazelcastTestSupport {

    @Rule
    public HotRestartFolderRule hotRestartFolderRule = new HotRestartFolderRule();

    protected TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

    void shutdown(final HazelcastInstance hz1, HazelcastInstance hz2) {
        hz2.getCluster().shutdown();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertFalse(hz1.getLifecycleService().isRunning());
            }
        });
    }

    Config newConfig() {
        Config config = new Config();
        config.getHotRestartPersistenceConfig().setEnabled(true).setBaseDir(hotRestartFolderRule.getBaseDir())
                .setClusterDataRecoveryPolicy(HotRestartClusterDataRecoveryPolicy.PARTIAL_RECOVERY_MOST_COMPLETE);
        return config;
    }

    public static ClusterHotRestartStatusDTO getClusterHotRestartStatus(HazelcastInstance instance) {
        InternalHotRestartService internalHotRestartService = getNode(instance).getNodeExtension().getInternalHotRestartService();
        return internalHotRestartService.getCurrentClusterHotRestartStatus();
    }
}
