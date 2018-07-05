package com.hazelcast.wan.fw;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.wan.PartitionWanEventContainer;
import com.hazelcast.enterprise.wan.PublisherQueueContainer;
import com.hazelcast.enterprise.wan.replication.WanBatchReplication;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.AssertTask;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.wan.fw.WanTestSupport.wanReplicationEndpoint;
import static com.hazelcast.wan.fw.WanTestSupport.wanReplicationService;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;

public class WanCounterTestSupport {
    private WanCounterTestSupport() {
    }

    public static void verifyEventCountersAreEventuallyZero(final Cluster sourceCluster, final WanReplication wanReplication) {
        final String targetGroupName = wanReplication.getTargetClusterName();
        final String replicaName = wanReplication.getSetupName();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : sourceCluster.getMembers()) {
                    if (instance != null && instance.getLifecycleService().isRunning()) {
                        int outboundQueueSize = getPrimaryOutboundQueueSize(instance, replicaName, targetGroupName);

                        WanBatchReplication endpoint = wanReplicationEndpoint(instance, wanReplication);
                        int outboundBackupQueueSize = endpoint.getCurrentBackupElementCount();

                        assertEquals(0, outboundQueueSize);
                        assertEquals(0, outboundBackupQueueSize);
                    }
                }
            }
        });
    }

    public static ScheduledFuture<?> dumpWanCounters(WanReplication wanReplication, ScheduledExecutorService executorService) {
        WanCounterDumper wanCounterDumper = new WanCounterDumper(wanReplication);
        return executorService.scheduleAtFixedRate(wanCounterDumper, 0, 500, MILLISECONDS);
    }

    public static void stopDumpingWanCounters(ScheduledFuture<?> dumpFuture) {
        dumpFuture.cancel(false);
    }

    private static class WanCounterDumper implements Runnable {
        private ILogger logger = Logger.getLogger(WanCounterDumper.class);

        private final WanReplication wanReplication;

        private WanCounterDumper(WanReplication wanReplication) {
            this.wanReplication = wanReplication;
        }

        @Override
        public void run() {
            dumpWanCounters(wanReplication);
        }

        private void dumpWanCounters(WanReplication wanReplication) {
            String setupName = wanReplication.getSetupName();
            String targetClusterName = wanReplication.getTargetClusterName();
            logger.info(
                    "===[" + setupName + "(" + targetClusterName + ")]=======================================================");
            int sumOfTotalPublishedEvents = 0;
            for (HazelcastInstance instance : wanReplication.getSourceCluster().getMembers()) {
                if (instance != null && instance.getLifecycleService().isRunning()) {
                    String instanceName = instance.getName();
                    WanBatchReplication endpoint = wanReplicationEndpoint(instance, wanReplication);

                    int outboundQueueSize = getPrimaryOutboundQueueSize(instance, setupName, targetClusterName);
                    logger.info("PRIMARY counter on " + instanceName + ": " + outboundQueueSize);

                    logger.info("BACKUP counter on " + instanceName + ": " + endpoint.getCurrentBackupElementCount());

                    int outboundQueueSizes = getQueueSizes(instance, endpoint, true);
                    logger.info("PRIMARY Q size on " + instanceName + ": " + outboundQueueSizes);

                    int outboundAllQueueSizes = getQueueSizes(instance, endpoint, false);
                    logger.info("ALL Q size on " + instanceName + ": " + outboundAllQueueSizes);

                    long totalPublishedEventCount = getTotalPublishedEventCount(instance, setupName, targetClusterName);
                    logger.info("Total events published on " + instanceName + ": " + totalPublishedEventCount);

                    sumOfTotalPublishedEvents += totalPublishedEventCount;
                }
            }
            logger.info("Sum of total events by the members: " + sumOfTotalPublishedEvents);
        }

    }

    private static int getQueueSizes(HazelcastInstance instance, WanBatchReplication endpoint, boolean onlyPrimaries) {
        PublisherQueueContainer b = endpoint.getPublisherQueueContainer();
        int queueSizes = 0;

        InternalPartitionService partitionService = ((HazelcastInstanceProxy) instance).getOriginal().node.getPartitionService();

        for (Map.Entry<Integer, PartitionWanEventContainer> eventEntry : b.getPublisherEventQueueMap().entrySet()) {
            Integer partitionId = eventEntry.getKey();
            InternalPartition partition = partitionService.getPartition(partitionId);
            if (!onlyPrimaries || partition.isLocal()) {
                PartitionWanEventContainer container = eventEntry.getValue();
                queueSizes += container.size();
            }
        }

        return queueSizes;
    }

    private static long getTotalPublishedEventCount(HazelcastInstance instance, String replicaName, String targetName) {
        return wanReplicationService(instance).getStats()
                                        .get(replicaName).getLocalWanPublisherStats()
                                        .get(targetName).getTotalPublishedEventCount();
    }

    private static int getPrimaryOutboundQueueSize(HazelcastInstance instance, String replicaName, String targetName) {
        return wanReplicationService(instance).getStats()
                                              .get(replicaName).getLocalWanPublisherStats()
                                              .get(targetName).getOutboundQueueSize();
    }

}
