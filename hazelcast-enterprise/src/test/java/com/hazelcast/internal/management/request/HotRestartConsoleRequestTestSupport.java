package com.hazelcast.internal.management.request;

import com.hazelcast.config.Config;
import com.hazelcast.config.HotRestartClusterDataRecoveryPolicy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.hotrestart.InternalHotRestartService;
import com.hazelcast.internal.management.dto.ClusterHotRestartStatusDTO;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

import java.io.File;
import java.io.IOException;

import static com.hazelcast.nio.IOUtil.toFileName;
import static org.junit.Assert.assertFalse;

public abstract class HotRestartConsoleRequestTestSupport extends HazelcastTestSupport {

    @Rule
    public TestName testName = new TestName();

    protected File baseDir;
    protected TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

    @Before
    public void before() throws IOException {
        baseDir = new File(toFileName(getClass().getSimpleName()) + '_' + toFileName(testName.getMethodName()));
        IOUtil.delete(baseDir);
        if (!baseDir.mkdir()) {
            throw new IllegalStateException("Failed to create hot-restart directory!");
        }
    }

    @After
    public void after() throws IOException {
        factory.terminateAll();
        IOUtil.delete(baseDir);
    }

    void shutdown(final HazelcastInstance hz1, HazelcastInstance hz2) {
        hz2.getCluster().shutdown();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertFalse(hz1.getLifecycleService().isRunning());
            }
        });
    }

    Config newConfig(String dir) {
        Config config = new Config();
        config.getHotRestartPersistenceConfig().setEnabled(true).setBaseDir(new File(baseDir, dir))
                .setClusterDataRecoveryPolicy(HotRestartClusterDataRecoveryPolicy.PARTIAL_RECOVERY_MOST_COMPLETE);
        return config;
    }

    public static ClusterHotRestartStatusDTO getClusterHotRestartStatus(HazelcastInstance instance) {
        InternalHotRestartService internalHotRestartService = getNode(instance).getNodeExtension().getInternalHotRestartService();
        return internalHotRestartService.getCurrentClusterHotRestartStatus();
    }
}
