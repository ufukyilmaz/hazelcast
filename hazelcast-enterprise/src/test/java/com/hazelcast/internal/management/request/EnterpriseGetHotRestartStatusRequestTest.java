/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.management.request;

import com.hazelcast.config.Config;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.internal.management.dto.ClusterHotRestartStatusDTO;
import com.hazelcast.internal.management.dto.ClusterHotRestartStatusDTO.ClusterHotRestartStatus;
import com.hazelcast.internal.management.dto.ClusterHotRestartStatusDTO.MemberHotRestartStatus;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.hotrestart.cluster.ClusterHotRestartEventListener;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.config.HotRestartClusterDataRecoveryPolicy.FULL_RECOVERY_ONLY;
import static com.hazelcast.config.HotRestartClusterDataRecoveryPolicy.PARTIAL_RECOVERY_MOST_COMPLETE;
import static com.hazelcast.internal.management.GetHotRestartStatusRequestTest.getClusterHotRestartStatus;
import static com.hazelcast.internal.management.dto.ClusterHotRestartStatusDTO.MemberHotRestartStatus.LOAD_IN_PROGRESS;
import static com.hazelcast.internal.management.dto.ClusterHotRestartStatusDTO.MemberHotRestartStatus.PENDING;
import static java.util.Collections.synchronizedCollection;
import static org.hamcrest.Matchers.isOneOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class EnterpriseGetHotRestartStatusRequestTest extends HotRestartConsoleRequestTestSupport {

    @Test
    public void testGetStatus_withoutHotRestart() throws Exception {
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
        final String dir_hz1 = "hz_1";
        HazelcastInstance hz1 = factory.newHazelcastInstance(newConfig(dir_hz1));

        final String dir_hz2 = "hz_2";
        HazelcastInstance hz2 = factory.newHazelcastInstance(newConfig(dir_hz2));

        assertClusterSizeEventually(2, hz1);
        warmUpPartitions(hz2);
        shutdown(hz1, hz2);

        final GetHotRestartStatusListener listener = new GetHotRestartStatusListener();

        Future<HazelcastInstance> future1 = startHazelcastInstanceAsync(dir_hz1, listener);
        Future<HazelcastInstance> future2 = startHazelcastInstanceAsync(dir_hz2, listener);

        ClusterHotRestartStatusDTO loadingClusterHotRestartStatus = listener.getCurrentClusterHotRestartStatus();
        listener.unblockHotRestart();

        assertEquals(PARTIAL_RECOVERY_MOST_COMPLETE, loadingClusterHotRestartStatus.getDataRecoveryPolicy());
        assertEquals(ClusterHotRestartStatus.IN_PROGRESS, loadingClusterHotRestartStatus.getHotRestartStatus());
        Map<String, MemberHotRestartStatus> memberHotRestartStatusMap = loadingClusterHotRestartStatus.getMemberHotRestartStatusMap();
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

    private Future<HazelcastInstance> startHazelcastInstanceAsync(final String dir,
            final GetHotRestartStatusListener listener) {

        return spawn(new Callable<HazelcastInstance>() {
                @Override
                public HazelcastInstance call() throws Exception {
                    Config config = newConfig(dir);
                    config.addListenerConfig(new ListenerConfig(listener));
                    return factory.newHazelcastInstance(config);
                }
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
        private final Collection<HazelcastInstance> instances = synchronizedCollection(new ArrayList<HazelcastInstance>());

        @Override
        public void onDataLoadStart(Address address) {
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

        ClusterHotRestartStatusDTO getCurrentClusterHotRestartStatus() throws Exception {
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
