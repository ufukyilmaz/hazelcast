package com.hazelcast.enterprise.wan.impl.replication;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.internal.diagnostics.StoreLatencyPlugin;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.util.executor.CompletedFuture;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LatencyTrackingWanBatchSenderTest extends HazelcastTestSupport {

    private static final String GROUP_NAME = "groupName";

    private StoreLatencyPlugin plugin;
    private WanBatchSender delegate;
    private LatencyTrackingWanBatchSender wanBatchSender;

    @Before
    public void setup() {
        final HazelcastInstance hz = createHazelcastInstance();
        plugin = new StoreLatencyPlugin(getNodeEngineImpl(hz));
        delegate = mock(WanBatchSender.class);
        wanBatchSender = new LatencyTrackingWanBatchSender(delegate, plugin, GROUP_NAME,
                Executors.newSingleThreadExecutor());
    }

    @Test
    public void send() throws UnknownHostException, ExecutionException, InterruptedException {
        final Address successfulHost = new Address("localhost", 1234);
        final Address failingHost = new Address("localhost", 1235);
        final BatchWanReplicationEvent batchEvent = new BatchWanReplicationEvent(false);
        when(delegate.send(batchEvent, successfulHost))
                .thenReturn(new CompletedFuture<Boolean>(null, true, null));
        when(delegate.send(batchEvent, failingHost))
                .thenReturn(new CompletedFuture<Boolean>(null, false, null));

        assertTrue(wanBatchSender.send(batchEvent, successfulHost).get());
        assertFalse(wanBatchSender.send(batchEvent, failingHost).get());
        assertProbeCalledEventually(failingHost, 1);
        assertProbeCalledEventually(successfulHost, 1);
    }

    private void assertProbeCalledEventually(final Address address, long times) {
        assertEqualsEventually(new Callable<Long>() {
            @Override
            public Long call() {
                return plugin.count(LatencyTrackingWanBatchSender.KEY, GROUP_NAME, address.toString());

            }
        }, times);
    }
}
