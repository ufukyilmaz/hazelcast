package com.hazelcast.wan.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.ConsistencyCheckStrategy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.map.IMap;
import com.hazelcast.enterprise.EnterpriseSerialParametersRunnerFactory;
import com.hazelcast.enterprise.wan.impl.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.impl.replication.WanBatchReplication;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.util.RandomPicker;
import com.hazelcast.wan.impl.WanSyncStatus;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.config.ConsistencyCheckStrategy.MERKLE_TREES;
import static com.hazelcast.config.ConsistencyCheckStrategy.NONE;
import static com.hazelcast.wan.fw.WanTestSupport.wanReplicationService;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseSerialParametersRunnerFactory.class)
@Category(SlowTest.class)
public class HDMapWanSyncTest extends MapWanReplicationTestSupport {

    @Parameterized.Parameters(name = "consistencyCheckStrategy:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {NONE},
                {MERKLE_TREES}
        });
    }

    @Parameterized.Parameter
    public ConsistencyCheckStrategy consistencyCheckStrategy;

    private volatile boolean running = true;

    @Override
    protected Config getConfig() {
        final Config config = super.getConfig();
        config.getMapConfig("default")
              .setInMemoryFormat(getMemoryFormat());
        if (consistencyCheckStrategy == MERKLE_TREES) {
            config.getMapConfig("default").getMerkleTreeConfig()
                  .setEnabled(true)
                  .setDepth(5);
        }
        return config;
    }

    /* Accessing native memory from threads other than the partition thread was causing JVM crash.
       See
     */
    @Test
    public void checkMemoryAccessSafety() throws InterruptedException {
        configA.getNativeMemoryConfig().setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.STANDARD);
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName(),
                consistencyCheckStrategy);
        startClusterA();
        startClusterB();
        warmUpPartitions(clusterA);
        warmUpPartitions(clusterB);

        final CountDownLatch startLatch = new CountDownLatch(1);
        final IMap<Integer, byte[]> map = getNode(clusterA).getMap("map");

        spawn(new Runnable() {
            @Override
            public void run() {
                while (running) {
                    int keyCount = 10000;
                    if (RandomPicker.getInt(keyCount) > 80) {
                        map.delete(RandomPicker.getInt(keyCount));
                    } else {
                        map.set(RandomPicker.getInt(keyCount), new byte[RandomPicker.getInt(10, 8000)]);
                    }
                }
                startLatch.countDown();
            }
        });

        final EnterpriseWanReplicationService wanReplicationService = wanReplicationService(clusterA[0]);
        for (int i = 0; i < 100; i++) {
            wanReplicationService.syncMap("atob", "B", "map");
            assertTrueEventually(new AssertTask() {
                @Override
                public void run() {
                    assertEquals(WanSyncStatus.READY, wanReplicationService.getWanSyncState().getStatus());
                }
            });
        }
        running = false;
        startLatch.await();

        int size = map.size();
        wanReplicationService.syncMap("atob", "B", "map");
        assertDataSizeEventually(clusterB, "map", size);
    }

    @After
    @Override
    public void cleanup() {
        super.cleanup();
        running = false;
    }

    @Override
    public String getReplicationImpl() {
        return WanBatchReplication.class.getName();
    }

    @Override
    public InMemoryFormat getMemoryFormat() {
        return InMemoryFormat.NATIVE;
    }
}
