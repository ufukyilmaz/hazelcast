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
import com.hazelcast.core.Member;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.spi.hotrestart.cluster.ClusterHotRestartEventListener;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.management.TriggerPartialStartRequestTest.triggerPartialStart;
import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class EnterpriseTriggerPartialStartRequestTest extends HotRestartConsoleRequestTestSupport {

    @Test
    public void testTriggerPartialStart_withoutHotRestart() throws Exception {
        HazelcastInstance hz = factory.newHazelcastInstance();
        String result = triggerPartialStart(hz);
        assertEquals(TriggerPartialStartRequest.FAILED_RESULT, result);
    }

    @Test
    public void testTriggerPartialStart() throws Exception {
        String dir_hz1 = "hz_1";
        final HazelcastInstance hz1 = factory.newHazelcastInstance(newConfig(dir_hz1));

        String dir_hz2 = "hz_2";
        HazelcastInstance hz2 = factory.newHazelcastInstance(newConfig(dir_hz2));

        assertClusterSizeEventually(2, hz1);
        shutdown(hz1, hz2);

        final Config config = newConfig(dir_hz1);
        TriggerPartialStartViaManagementCenterListener listener = new TriggerPartialStartViaManagementCenterListener();
        config.addListenerConfig(new ListenerConfig(listener));

        spawn(new Callable() {
            @Override
            public Object call() throws Exception {
                factory.newHazelcastInstance(config);
                return null;
            }
        });

        String result = listener.getResponse();
        assertEquals(TriggerPartialStartRequest.SUCCESS_RESULT, result);
    }

    private static class TriggerPartialStartViaManagementCenterListener extends ClusterHotRestartEventListener
            implements HazelcastInstanceAware {

        private final CountDownLatch latch = new CountDownLatch(1);
        private volatile HazelcastInstance hz;
        private volatile String response;

        @Override
        public void beforeAllMembersJoin(Collection<? extends Member> currentMembers) {
            try {
                response = triggerPartialStart(hz);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                latch.countDown();
            }
        }

        String getResponse() throws InterruptedException {
            latch.await(ASSERT_TRUE_EVENTUALLY_TIMEOUT, TimeUnit.SECONDS);
            return response;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hz = hazelcastInstance;
        }
    }
}
