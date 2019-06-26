package com.hazelcast.test;

import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.wan.impl.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.WanReplicationEndpoint;
import com.hazelcast.enterprise.wan.impl.WanReplicationPublisherDelegate;
import com.hazelcast.enterprise.wan.impl.replication.WanBatchReplication;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.monitor.LocalWanPublisherStats;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.wan.DistributedServiceWanEventCounters;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.util.StringUtil.LINE_SEPARATOR;

public final class WanStatisticsRule implements TestRule {
    private static final String STATS_DISABLED_MESSAGE = "Statistics explicitly disabled";
    private static final String EXCEPTION_WHILE_SNAPSHOTTING_STATS_MESSSAGE = "Exception while snapshotting WAN stats";

    private String stats;
    private Throwable throwable;

    @Override
    public Statement apply(final Statement base, final Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                String friendlyName = description.getDisplayName();
                try {
                    base.evaluate();

                    // fail-fast when statistics are not collected after a test finished
                    // this is most likely caused by omitting the snapshotStats() call
                    assertHasStats(friendlyName);
                } catch (Throwable t) {
                    onFailure(friendlyName);
                    throw t;
                }
            }
        };
    }

    public void snapshotStats(TestHazelcastInstanceFactory factory) {
        if (stats == null) {
            try {
                stats = prepareStats(factory);
            } catch (Throwable t) {
                stats = EXCEPTION_WHILE_SNAPSHOTTING_STATS_MESSSAGE;
                throwable = t;
            }
        } else if (!STATS_DISABLED_MESSAGE.equals(stats)) {
            throw new AssertionError("Calling snapshot multiple times is not allowed");
        }
    }

    public void disableStats() {
        stats = STATS_DISABLED_MESSAGE;
    }

    public void onFailure(String friendlyName) {
        if (stats == null) {
            // there was a real failure, but statistics are missing. just print a failure.
            // this should not happened as we fail a passing test when statistics are missing
            System.out.println("Test '" + friendlyName + "' failed to collect statistics!");
        } else {
            System.out.println("Test '" + friendlyName + "' WAN statistics:" + LINE_SEPARATOR + stats);
            if (throwable != null) {
                throwable.printStackTrace();
            }

        }
    }

    private void assertHasStats(String friendlyName) {
        if (stats == null) {
            throw new AssertionError("Test '" + friendlyName
                    + "' You have to call 'snapshotStats()' when using the WanStatisticsRule. "
                    + "The @After method is the best place for this. This is needed as otherwise "
                    + HazelcastTestSupport.class + " terminates all instances before this rule has a chance to extract"
                    + " WAN statistics. If you do not want to dump statistics for selected tests then you can call method"
                    + " disableStats(). ");
        }
    }

    private String prepareStats(TestHazelcastInstanceFactory factory) {
        Collection<HazelcastInstance> instances = factory.getAllHazelcastInstances();
        StringBuilder sb = new StringBuilder();
        if (instances.isEmpty()) {
            sb.append("Found instance factory " + factory + ", but it has no live instance");
            return null;
        }

        sb.append("----- WAN STATS START -----").append(LINE_SEPARATOR);
        for (HazelcastInstance instance : instances) {
            sb.append(instance).append(LINE_SEPARATOR);
            Node node = HazelcastTestSupport.getNode(instance);
            NodeEngine nodeEngine = node.nodeEngine;
            final EnterpriseWanReplicationService s = nodeEngine.getService(EnterpriseWanReplicationService.SERVICE_NAME);
            Set<String> replicationNames = s.getStats().keySet();
            for (String replicationName : replicationNames) {
                sb.append("Replication: ").append(replicationName).append(LINE_SEPARATOR);
                WanReplicationPublisherDelegate publisherDelegate = (WanReplicationPublisherDelegate) s.getWanReplicationPublisher(replicationName);
                Collection<WanReplicationEndpoint> endpoints = publisherDelegate.getEndpoints();
                for (WanReplicationEndpoint endpoint : endpoints) {
                    if (endpoint instanceof WanBatchReplication) {
                        WanBatchReplication batchReplicationEndPoint = (WanBatchReplication) endpoint;
                        LocalWanPublisherStats stats = batchReplicationEndPoint.getStats();
                        long failedTransmissionCount = batchReplicationEndPoint.getFailedTransmissionCount();
                        sb.append("Target endpoints: ").append(batchReplicationEndPoint.getTargetEndpoints())
                                .append(", Stats: ").append(stats)
                                .append(", Failed transmission count: ").append(failedTransmissionCount).append(LINE_SEPARATOR);
                    } else {
                        sb.append("Unknown endpoint type ").append(endpoint).append(", cannot collect stats.");
                        sb.append(LINE_SEPARATOR);
                    }
                }
            }
            if (replicationNames.isEmpty()) {
                sb.append("[No replication configured]").append(LINE_SEPARATOR);
            }

            renderCounters(sb, s);

            sb.append(LINE_SEPARATOR);
        }
        sb.append("----- WAN STATS END -----");
        return sb.toString();
    }

    private void renderCounters(StringBuilder sb, EnterpriseWanReplicationService s) {
        DistributedServiceWanEventCounters counters = s.getReceivedEventCounters(MapService.SERVICE_NAME);
        renderCounterMap(sb, counters.getEventCounterMap(), "Map");

        counters = s.getReceivedEventCounters(CacheService.SERVICE_NAME);
        renderCounterMap(sb, counters.getEventCounterMap(), "Cache");
    }

    private void renderCounterMap(StringBuilder sb, Map<String,
            DistributedServiceWanEventCounters.DistributedObjectWanEventCounters> eventCounterMap,
                                  String structureType) {
        for (Map.Entry<String, DistributedServiceWanEventCounters.DistributedObjectWanEventCounters> entry : eventCounterMap.entrySet()) {
            DistributedServiceWanEventCounters.DistributedObjectWanEventCounters value = entry.getValue();
            long removeCount = value.getRemoveCount();
            long syncCount = value.getSyncCount();
            long updateCount = value.getUpdateCount();
            long droppedCount = value.getDroppedCount();
            sb.append(structureType).append(": '").append(entry.getKey()).append("', Receive Counters: ")
                    .append(" Remove count: ").append(removeCount)
                    .append(", Sync count: ").append(syncCount)
                    .append(", Update count: ").append(updateCount)
                    .append(", Dropped count: ").append(droppedCount)
                    .append(LINE_SEPARATOR);
        }
    }
}
