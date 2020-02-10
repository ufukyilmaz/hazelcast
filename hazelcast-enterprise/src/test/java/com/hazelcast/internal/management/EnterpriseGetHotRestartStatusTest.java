package com.hazelcast.internal.management;

import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.internal.hotrestart.cluster.ClusterHotRestartEventListener;
import com.hazelcast.internal.management.dto.ClusterHotRestartStatusDTO;
import com.hazelcast.internal.management.dto.ClusterHotRestartStatusDTO.ClusterHotRestartStatus;
import com.hazelcast.internal.management.dto.ClusterHotRestartStatusDTO.MemberHotRestartStatus;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.config.HotRestartClusterDataRecoveryPolicy.FULL_RECOVERY_ONLY;
import static com.hazelcast.config.HotRestartClusterDataRecoveryPolicy.PARTIAL_RECOVERY_MOST_COMPLETE;
import static com.hazelcast.internal.management.dto.ClusterHotRestartStatusDTO.MemberHotRestartStatus.LOAD_IN_PROGRESS;
import static com.hazelcast.internal.management.dto.ClusterHotRestartStatusDTO.MemberHotRestartStatus.PENDING;
import static com.hazelcast.test.Accessors.getNode;
import static java.util.Collections.synchronizedCollection;
import static org.hamcrest.Matchers.isOneOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EnterpriseGetHotRestartStatusTest extends HotRestartConsoleTestSupport {

    @Test
    public void testGetStatus_withoutHotRestart() {
        HazelcastInstance hz = factory.newHazelcastInstance();
        ClusterHotRestartStatusDTO clusterHotRestartStatus = getClusterHotRestartStatus(hz);

        assertEquals(FULL_RECOVERY_ONLY, clusterHotRestartStatus.getDataRecoveryPolicy());
        assertEquals(ClusterHotRestartStatus.UNKNOWN, clusterHotRestartStatus.getHotRestartStatus());
        assertEquals(-1, clusterHotRestartStatus.getRemainingValidationTimeMillis());
        assertEquals(-1, clusterHotRestartStatus.getRemainingDataLoadTimeMillis());
        assertTrue(clusterHotRestartStatus.getMemberHotRestartStatusMap().isEmpty());
    }

    @Test
    public void testGetStatus() throws Exception {
        HazelcastInstance hz1 = factory.newHazelcastInstance(newConfig());
        HazelcastInstance hz2 = factory.newHazelcastInstance(newConfig());

        assertClusterSize(2, hz1, hz2);
        warmUpPartitions(hz2);
        shutdown(hz1, hz2);

        final GetHotRestartStatusListener listener = new GetHotRestartStatusListener();

        Future<HazelcastInstance> future1 = startHazelcastInstanceAsync(listener);
        Future<HazelcastInstance> future2 = startHazelcastInstanceAsync(listener);

        ClusterHotRestartStatusDTO loadingClusterHotRestartStatus = listener.getCurrentClusterHotRestartStatus();
        listener.unblockHotRestart();

        assertEquals(PARTIAL_RECOVERY_MOST_COMPLETE, loadingClusterHotRestartStatus.getDataRecoveryPolicy());
        assertEquals(ClusterHotRestartStatus.IN_PROGRESS, loadingClusterHotRestartStatus.getHotRestartStatus());
        Map<String, MemberHotRestartStatus> memberHotRestartStatusMap
                = loadingClusterHotRestartStatus.getMemberHotRestartStatusMap();
        assertEquals(2, memberHotRestartStatusMap.size());
        for (MemberHotRestartStatus status : memberHotRestartStatusMap.values()) {
            assertThat(status, isOneOf(PENDING, LOAD_IN_PROGRESS));
        }

        hz1 = future1.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, TimeUnit.SECONDS);
        hz2 = future2.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, TimeUnit.SECONDS);

        HazelcastInstance master = getNode(hz1).isMaster() ? hz1 : hz2;

        ClusterHotRestartStatusDTO finalClusterHotRestartStatus = getClusterHotRestartStatus(master);
        assertHotRestartCompleted(finalClusterHotRestartStatus);
    }

    private Future<HazelcastInstance> startHazelcastInstanceAsync(final GetHotRestartStatusListener listener) {
        return spawn(() -> {
            Config config = newConfig();
            config.addListenerConfig(new ListenerConfig(listener));
            return factory.newHazelcastInstance(config);
        });
    }

    private void assertHotRestartCompleted(ClusterHotRestartStatusDTO clusterHotRestartStatus) {
        assertEquals(ClusterHotRestartStatus.SUCCEEDED, clusterHotRestartStatus.getHotRestartStatus());
        Map<String, MemberHotRestartStatus> memberHotRestartStatusMap = clusterHotRestartStatus.getMemberHotRestartStatusMap();
        assertEquals(2, memberHotRestartStatusMap.size());
        for (MemberHotRestartStatus status : memberHotRestartStatusMap.values()) {
            assertEquals(MemberHotRestartStatus.SUCCESSFUL, status);
        }
    }

    private static class GetHotRestartStatusListener extends ClusterHotRestartEventListener
            implements HazelcastInstanceAware {

        private CountDownLatch loadStartLatch = new CountDownLatch(2);
        private CountDownLatch latch = new CountDownLatch(1);
        private final Collection<HazelcastInstance> instances = synchronizedCollection(new ArrayList<>());

        @Override
        public void onDataLoadStart(Member member) {
            loadStartLatch.countDown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            instances.add(hazelcastInstance);
        }

        void unblockHotRestart() {
            latch.countDown();
        }

        ClusterHotRestartStatusDTO getCurrentClusterHotRestartStatus() {
            assertOpenEventually(loadStartLatch);
            HazelcastInstance master = null;
            for (HazelcastInstance instance : instances) {
                if (getNode(instance).isMaster()) {
                    master = instance;
                }
            }
            assertNotNull(master);
            return getClusterHotRestartStatus(master);
        }
    }
}
