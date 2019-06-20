package com.hazelcast.wan.fw;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.wan.PartitionWanEventContainer;
import com.hazelcast.enterprise.wan.replication.WanBatchReplication;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.ProgressCheckerTask;
import com.hazelcast.test.TaskProgress;
import com.hazelcast.wan.WanSyncStats;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import static com.hazelcast.test.HazelcastTestSupport.assertCompletesEventually;
import static com.hazelcast.wan.fw.WanTestSupport.wanReplicationEndpoint;
import static com.hazelcast.wan.fw.WanTestSupport.wanReplicationService;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;

public class WanCounterTestSupport {
    private static final ILogger LOGGER = Logger.getLogger(WanCounterTestSupport.class);

    private WanCounterTestSupport() {
    }

    public static void verifyEventCountersAreEventuallyZero(final Cluster cluster, final WanReplication wanReplication) {
        LOGGER.info("Start verifying that WAN counters reach zero");
        assertCompletesEventually(new QueueDrainingProgressCheckerTask(cluster, wanReplication));
    }

    public static int getClusterWideSumPrimaryCounter(Cluster cluster, WanReplication wanReplication) {
        int sumPrimary = 0;

        for (HazelcastInstance instance : cluster.getMembers()) {
            if (instance != null && instance.getLifecycleService().isRunning()) {
                sumPrimary += getPrimaryOutboundQueueSize(instance, wanReplication);
            }
        }

        return sumPrimary;
    }

    public static int getClusterWideSumBackupCounter(Cluster cluster, WanReplication wanReplication) {
        int sumBackup = 0;

        for (HazelcastInstance instance : cluster.getMembers()) {
            if (instance != null && instance.getLifecycleService().isRunning()) {
                sumBackup += getBackupOutboundQueueSize(instance, wanReplication);
            }
        }

        return sumBackup;
    }

    public static int getClusterWideSumPublishedCounter(Cluster cluster, WanReplication wanReplication) {
        int sumPublished = 0;

        for (HazelcastInstance instance : cluster.getMembers()) {
            if (instance != null && instance.getLifecycleService().isRunning()) {
                sumPublished += getTotalPublishedEventCount(instance, wanReplication);
            }
        }

        return sumPublished;
    }

    public static void verifyEventCountersAreZero(Cluster sourceCluster, WanReplication wanReplication) {
        for (HazelcastInstance instance : sourceCluster.getMembers()) {
            if (instance != null && instance.getLifecycleService().isRunning()) {
                WanBatchReplication endpoint = wanReplicationEndpoint(instance, wanReplication);
                int primaryQueueSize = getPrimaryOutboundQueueSize(instance, wanReplication);
                int backupQueueSize = endpoint.getCurrentBackupElementCount();

                String instanceName = instance.getName();
                assertEquals("Primary WAN queue on member " + instanceName + " hasn't reached zero", 0, primaryQueueSize);
                assertEquals("Backup WAN queue on member " + instanceName + " hasn't reached zero", 0, backupQueueSize);
            }
        }
    }

    public static int getClusterWideSumPartitionsSyncedCount(Cluster cluster, WanReplication wanReplication, String mapName) {
        int sumPrimary = 0;

        for (HazelcastInstance instance : cluster.getMembers()) {
            if (instance != null && instance.getLifecycleService().isRunning()) {
                WanSyncStats syncStats = getSyncStats(instance, wanReplication, mapName);
                sumPrimary += syncStats != null ? syncStats.getPartitionsSynced() : 0;
            }
        }

        return sumPrimary;
    }

    public static ScheduledFuture<?> dumpWanCounters(WanReplication wanReplication, ScheduledExecutorService executorService) {
        WanCounterDumper wanCounterDumper = new WanCounterDumper(wanReplication);
        return executorService.scheduleAtFixedRate(wanCounterDumper, 0, 1000, MILLISECONDS);
    }

    public static void stopDumpingWanCounters(ScheduledFuture<?> dumpFuture) {
        dumpFuture.cancel(false);
    }

    private static int getQueueSizes(HazelcastInstance instance,
                                     WanBatchReplication endpoint,
                                     boolean onlyPrimaries) {
        int queueSizes = 0;

        InternalPartitionService partitionService = ((HazelcastInstanceProxy) instance).getOriginal().node.getPartitionService();
        PartitionWanEventContainer[] containers = endpoint.getEventQueueContainer()
                                                          .getContainers();
        for (int partitionId = 0; partitionId < containers.length; partitionId++) {
            PartitionWanEventContainer container = containers[partitionId];
            InternalPartition partition = partitionService.getPartition(partitionId);
            if (!onlyPrimaries || partition.isLocal()) {
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

    private static long getTotalPublishedEventCount(HazelcastInstance instance, WanReplication wanReplication) {
        return wanReplicationEndpoint(instance, wanReplication).getStats().getTotalPublishedEventCount();
    }

    private static int getPrimaryOutboundQueueSize(HazelcastInstance instance, WanReplication wanReplication) {
        return wanReplicationEndpoint(instance, wanReplication).getCurrentElementCount();
    }

    private static int getBackupOutboundQueueSize(HazelcastInstance instance, WanReplication wanReplication) {
        return wanReplicationEndpoint(instance, wanReplication).getCurrentBackupElementCount();
    }

    private static WanSyncStats getSyncStats(HazelcastInstance instance, WanReplication wanReplication, String mapName) {
        WanSyncStats wanSyncStats = wanReplicationEndpoint(instance, wanReplication).getStats().getLastSyncStats().get(mapName);
        return wanSyncStats;
    }

    private static class QueueDrainingProgressCheckerTask implements ProgressCheckerTask {
        private final Cluster cluster;
        private final WanReplication wanReplication;

        QueueDrainingProgressCheckerTask(Cluster cluster, WanReplication wanReplication) {
            this.cluster = cluster;
            this.wanReplication = wanReplication;
        }

        @Override
        public TaskProgress checkProgress() {
            int currentPrimary = getClusterWideSumPrimaryCounter(cluster, wanReplication);
            int currentBackup = getClusterWideSumBackupCounter(cluster, wanReplication);
            int totalPublished = getClusterWideSumPublishedCounter(cluster, wanReplication);

            return new WanCounterTaskProgress(totalPublished, currentPrimary, currentBackup);
        }
    }

    private static class WanCounterTaskProgress implements TaskProgress {
        private final long timestamp = System.currentTimeMillis();
        private final int totalPublished;
        private final int currentPrimary;
        private final int currentBackup;
        private final int totalPrimaryEvents;

        WanCounterTaskProgress(int totalPublished, int currentPrimary, int currentBackup) {
            this.totalPublished = totalPublished;
            this.currentPrimary = currentPrimary;
            this.currentBackup = currentBackup;
            this.totalPrimaryEvents = totalPublished + currentPrimary;
        }

        @Override
        public boolean isCompleted() {
            return currentPrimary == 0 && currentBackup == 0;
        }

        @Override
        public double progress() {
            return Integer.MAX_VALUE - currentPrimary;
        }

        @Override
        public long timestamp() {
            return timestamp;
        }

        @Override
        public String getProgressString() {
            return String.format("Total WAN events: %d, published: %d, primary Q: %d backup Q: %d",
                    totalPrimaryEvents, totalPublished, currentPrimary, currentBackup);
        }

    }

    private static class WanCounterDumper implements Runnable {
        private ILogger logger = Logger.getLogger(WanCounterDumper.class);

        private final WanReplication wanReplication;

        private WanCounterDumper(WanReplication wanReplication) {
            this.wanReplication = wanReplication;
        }

        @Override
        public void run() {
            try {
                dumpWanCounters(wanReplication);
            } catch (Exception e) {
                logger.info("Error collecting counter values", e);
            }
        }

        private void dumpWanCounters(WanReplication wanReplication) {
            String setupName = wanReplication.getSetupName();
            String targetClusterName = wanReplication.getTargetClusterName();
            logger.info(
                    "===[" + setupName + "(" + targetClusterName + ")]=======================================================");
            int sumOfTotalPublishedEvents = 0;
            if (targetClusterName != null) {
                for (HazelcastInstance instance : wanReplication.getSourceCluster().getMembers()) {
                    if (instance != null && instance.getLifecycleService().isRunning()) {
                        String instanceName = instance.getName();
                        WanBatchReplication endpoint = wanReplicationEndpoint(instance, wanReplication);

                        int outboundQueueSize = getPrimaryOutboundQueueSize(instance, wanReplication);
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
            }
            logger.info("Sum of total events by the members: " + sumOfTotalPublishedEvents);
        }


    }
}
