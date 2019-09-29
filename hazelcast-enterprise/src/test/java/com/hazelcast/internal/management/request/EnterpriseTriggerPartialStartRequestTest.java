package com.hazelcast.internal.management.request;

import com.hazelcast.config.Config;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.cluster.Member;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.internal.hotrestart.cluster.ClusterHotRestartEventListener;
import com.hazelcast.test.annotation.ParallelJVMTest;
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
@Category({QuickTest.class, ParallelJVMTest.class})
public class EnterpriseTriggerPartialStartRequestTest extends HotRestartConsoleRequestTestSupport {

    @Test
    public void testTriggerPartialStart_withoutHotRestart() throws Exception {
        HazelcastInstance hz = factory.newHazelcastInstance();
        String result = triggerPartialStart(hz);
        assertEquals(TriggerPartialStartRequest.FAILED_RESULT, result);
    }

    @Test
    public void testTriggerPartialStart() throws Exception {
        final HazelcastInstance hz1 = factory.newHazelcastInstance(newConfig());
        HazelcastInstance hz2 = factory.newHazelcastInstance(newConfig());

        assertClusterSize(2, hz1, hz2);
        shutdown(hz1, hz2);

        final Config config = newConfig();
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
