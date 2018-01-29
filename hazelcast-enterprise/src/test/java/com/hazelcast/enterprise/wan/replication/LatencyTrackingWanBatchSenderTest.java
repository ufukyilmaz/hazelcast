package com.hazelcast.enterprise.wan.replication;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.enterprise.wan.BatchWanReplicationEvent;
import com.hazelcast.enterprise.wan.replication.LatencyTrackingWanBatchSender;
import com.hazelcast.enterprise.wan.replication.WanBatchSender;
import com.hazelcast.internal.diagnostics.StoreLatencyPlugin;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
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
        wanBatchSender = new LatencyTrackingWanBatchSender(delegate, plugin, GROUP_NAME);
    }

    @Test
    public void send() throws UnknownHostException {
        final Address successfulHost = new Address("localhost", 1234);
        final Address failingHost = new Address("localhost", 1235);
        final BatchWanReplicationEvent batchEvent = new BatchWanReplicationEvent(false);
        when(delegate.send(batchEvent, successfulHost)).thenReturn(true);
        when(delegate.send(batchEvent, failingHost)).thenReturn(false);

        assertEquals(wanBatchSender.send(batchEvent, successfulHost), true);
        assertEquals(wanBatchSender.send(batchEvent, failingHost), false);
        assertProbeCalled(failingHost, 1);
        assertProbeCalled(successfulHost, 1);
    }

    public void assertProbeCalled(Address address, int times) {
        assertEquals(times,
                plugin.count(LatencyTrackingWanBatchSender.KEY, GROUP_NAME, address.toString()));
    }
}