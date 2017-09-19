package com.hazelcast.wan.map;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.replication.WanBatchReplication;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.util.RandomPicker;
import com.hazelcast.wan.WanSyncStatus;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(SlowTest.class)
public class HdMapWanSyncTest extends AbstractMapWanSyncTest {

    private volatile boolean running = true;

    /* Accessing native memory from threads other than the partition thread was causing JVM crash.
       See
     */
    @Test
    public void checkMemoryAccessSafety() throws InterruptedException {
        configA.getNativeMemoryConfig().setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.STANDARD);
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName());
        startClusterA();
        startClusterB();
        warmUpPartitions(clusterA);
        warmUpPartitions(clusterB);

        final CountDownLatch startLatch = new CountDownLatch(1);

        final IMap map = getNode(clusterA).getMap("map");
        Executors.newSingleThreadExecutor().execute(new Runnable() {
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

        final EnterpriseWanReplicationService wanReplicationService
                = (EnterpriseWanReplicationService) getNode(clusterA[0]).nodeEngine.getWanReplicationService();
        for (int i = 0; i < 100; i++) {
            wanReplicationService.syncMap("atob", "B", "map");
            assertTrueEventually(new AssertTask() {
                @Override
                public void run() throws Exception {
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
