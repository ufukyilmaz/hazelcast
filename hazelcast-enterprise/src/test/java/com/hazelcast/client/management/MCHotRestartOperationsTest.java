package com.hazelcast.client.management;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.impl.management.ManagementCenterService;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.internal.hotrestart.cluster.ClusterHotRestartEventListener;
import com.hazelcast.internal.management.HotRestartConsoleTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.client.impl.management.ManagementCenterService.MC_CLIENT_MODE_PROP;
import static java.util.Arrays.stream;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
@Ignore
public class MCHotRestartOperationsTest extends HotRestartConsoleTestSupport {

    private static final int NODE_COUNT = 2;

    private TestHazelcastFactory factory;
    private HazelcastInstance[] instances = new HazelcastInstance[NODE_COUNT];
    private Member[] members;

    @Before
    public void setUp() {
        factory = new TestHazelcastFactory(NODE_COUNT);

        instances = factory.newInstances(newConfig());
        waitAllForSafeState(instances);

        members = stream(instances)
                .map(instance -> instance.getCluster().getLocalMember())
                .toArray(Member[]::new);
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void testTriggerPartialStart() throws Exception {
        shutdown(instances[0], instances[1]);

        Config config = newConfig();
        TriggerHotRestartViaMCListener listener =
                new TriggerHotRestartViaMCListener(() -> {
                    ManagementCenterService mcs = runClient();
                    return resolve(mcs.triggerPartialStart());
                });
        config.addListenerConfig(new ListenerConfig(listener));
        spawn(() -> factory.newHazelcastInstance(members[0].getAddress(), config));

        assertTrue(listener.getResult());
    }

    @Test
    public void testTriggerForceStart() throws Exception {
        shutdown(instances[0], instances[1]);

        Config config = newConfig();
        TriggerHotRestartViaMCListener listener =
                new TriggerHotRestartViaMCListener(() -> {
                    ManagementCenterService mcs = runClient();
                    return resolve(mcs.triggerForceStart());
                });
        config.addListenerConfig(new ListenerConfig(listener));
        spawn(() -> factory.newHazelcastInstance(members[0].getAddress(), config));

        assertTrue(listener.getResult());
    }

    @Test
    public void testGetTimedMemberState_onIncompleteStart() throws Exception {
        shutdown(instances[0], instances[1]);

        Config config = newConfig();
        TriggerHotRestartViaMCListener listener =
                new TriggerHotRestartViaMCListener(() -> {
                    ManagementCenterService mcs = runClient();
                    assertTrueEventually(() -> {
                        Optional<String> tmsJson = resolve(mcs.getTimedMemberState(members[0]));
                        assertTrue(tmsJson.isPresent());
                    });
                    return true;
                });
        config.addListenerConfig(new ListenerConfig(listener));
        spawn(() -> factory.newHazelcastInstance(members[0].getAddress(), config));

        assertTrue(listener.getResult());
    }

    @Test
    public void testTriggerHotRestartBackup() throws Exception {
        ManagementCenterService mcs = runClient();

        resolve(mcs.triggerHotRestartBackup());
        // intentionally no other asserts, it's enough when the test does not throw
    }

    @Test
    public void testInterruptHotRestartBackup() throws Exception {
        ManagementCenterService mcs = runClient();

        resolve(mcs.interruptHotRestartBackup());
        // intentionally no other asserts, it's enough when the test does not throw
    }

    private ManagementCenterService runClient() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(MC_CLIENT_MODE_PROP.getName(), "true");
        HazelcastInstance client = factory.newHazelcastClient(clientConfig);
        return ((HazelcastClientProxy) client).client.getManagementCenterService();
    }

    private static <T> T resolve(CompletableFuture<T> future) throws Exception {
        return future.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS);
    }

    private static class TriggerHotRestartViaMCListener extends ClusterHotRestartEventListener {

        private final CountDownLatch latch = new CountDownLatch(1);
        private final Callable<Boolean> operationCallable;
        private volatile boolean result;

        public TriggerHotRestartViaMCListener(Callable<Boolean> operationCallable) {
            this.operationCallable = operationCallable;
        }

        @Override
        public void beforeAllMembersJoin(Collection<? extends Member> currentMembers) {
            try {
                result = operationCallable.call();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                latch.countDown();
            }
        }

        boolean getResult() throws InterruptedException {
            latch.await(ASSERT_TRUE_EVENTUALLY_TIMEOUT, TimeUnit.SECONDS);
            return result;
        }
    }
}
