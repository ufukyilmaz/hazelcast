package com.hazelcast.enterprise.wan.replication;

import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.connection.WanConnectionWrapper;
import com.hazelcast.enterprise.wan.operation.MerkleTreeNodeValueComparison;
import com.hazelcast.enterprise.wan.operation.WanMerkleTreeNodeCompareOperation;
import com.hazelcast.enterprise.wan.sync.WanAntiEntropyEvent;
import com.hazelcast.enterprise.wan.sync.WanAntiEntropyEventResult;
import com.hazelcast.enterprise.wan.sync.WanConsistencyCheckEvent;
import com.hazelcast.enterprise.wan.sync.WanSyncEvent;
import com.hazelcast.enterprise.wan.sync.WanSyncManager;
import com.hazelcast.enterprise.wan.sync.WanSyncType;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MerkleTreeNodeEntries;
import com.hazelcast.map.impl.operation.MerkleTreeGetEntriesOperation;
import com.hazelcast.map.impl.operation.MerkleTreeNodeCompareOperationFactory;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationMerkleTreeNode;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationObject;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.spi.partition.IPartition;
import com.hazelcast.util.MapUtil;
import com.hazelcast.util.concurrent.BackoffIdleStrategy;
import com.hazelcast.util.concurrent.IdleStrategy;
import com.hazelcast.wan.WanReplicationEvent;
import com.hazelcast.wan.WanSyncStats;
import com.hazelcast.wan.merkletree.ConsistencyCheckResult;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.util.CollectionUtil.isEmpty;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Support class for processing WAN merkle tree anti-entropy events for a
 * single publisher.
 */
public class WanPublisherMerkleTreeSyncSupport implements WanPublisherSyncSupport {
    private static final int WAN_TARGET_INVOCATION_DEADLINE_SECONDS = 10;
    private static final int WAN_TARGET_INVOCATION_MIN_ATTEMPTS = 10;
    private static final long WAN_TARGET_INVOCATION_BACKOFF_MIN_PARK = MILLISECONDS.toNanos(1);
    private static final long WAN_TARGET_INVOCATION_BACKOFF_MAX_PARK = MILLISECONDS.toNanos(100);

    private final NodeEngineImpl nodeEngine;
    private final MapService mapService;
    private final WanConfigurationContext configurationContext;
    private final ILogger logger;
    private final Map<String, ConsistencyCheckResult> lastConsistencyCheckResults =
            new ConcurrentHashMap<String, ConsistencyCheckResult>();
    private final Map<String, WanSyncStats> lastSyncStats = new ConcurrentHashMap<String, WanSyncStats>();
    private final AbstractWanReplication publisher;
    private final WanSyncManager syncManager;
    /**
     * The count of {@link WanReplicationEvent} sync events pending replication per partition.
     */
    private final Map<Integer, AtomicInteger> counterMap = new ConcurrentHashMap<Integer, AtomicInteger>();
    /**
     * {@link IdleStrategy} used for
     */
    private final IdleStrategy wanTargetInvocationIdleStrategy;

    WanPublisherMerkleTreeSyncSupport(Node node,
                                      WanConfigurationContext configurationContext,
                                      AbstractWanReplication publisher) {
        this.nodeEngine = checkNotNull(node.getNodeEngine());
        this.mapService = nodeEngine.getService(MapService.SERVICE_NAME);
        this.logger = checkNotNull(node.getLogger(getClass()));
        this.configurationContext = checkNotNull(configurationContext);
        this.publisher = checkNotNull(publisher);
        final EnterpriseWanReplicationService service =
                checkNotNull((EnterpriseWanReplicationService) nodeEngine.getWanReplicationService());
        this.syncManager = checkNotNull(service.getSyncManager());
        this.wanTargetInvocationIdleStrategy = new BackoffIdleStrategy(0, 0, WAN_TARGET_INVOCATION_BACKOFF_MIN_PARK,
                WAN_TARGET_INVOCATION_BACKOFF_MAX_PARK);
    }

    /**
     * Processes the WAN merkle tree root check event and updates the
     * {@code result} with the processing results.
     *
     * @param event  WAN merkle tree root check event
     * @param result the processing result
     * @throws Exception if there was an exception when waiting for the results
     *                   of the merkle tree roots
     */
    public void processEvent(WanConsistencyCheckEvent event,
                             WanAntiEntropyEventResult result) throws Exception {
        String mapName = event.getMapName();
        String target = publisher.wanReplicationName + "/" + publisher.wanPublisherId;
        if (logger.isFineEnabled()) {
            logger.fine("Checking via Merkle trees if map " + mapName + " is consistent with cluster " + target);
        }
        lastConsistencyCheckResults.put(mapName, new ConsistencyCheckResult(-1, -1));
        ConsistencyCheckResult checkResult = new ConsistencyCheckResult();
        try {
            List<Integer> localPartitionsToSync = getLocalPartitions(event);
            Map<Integer, int[]> diff = compareMerkleTrees(mapName, localPartitionsToSync);
            if (diff != null) {
                checkResult = new ConsistencyCheckResult(localPartitionsToSync.size(), diff.size());
                result.addProcessedPartitions(localPartitionsToSync);
            }
        } finally {
            lastConsistencyCheckResults.put(mapName, checkResult);
        }

        if (logger.isFineEnabled()) {
            int checkedCount = checkResult.getLastCheckedPartitionCount();
            int diffCount = checkResult.getLastDiffPartitionCount();
            logger.fine("Consistency check for map " + mapName + " with cluster " + target + " has completed: "
                    + diffCount + " partitions out of " + checkedCount + " are not consistent");
        }
    }

    @Override
    public void removeReplicationEvent(EnterpriseMapReplicationObject sync) {
        EnterpriseMapReplicationMerkleTreeNode node = (EnterpriseMapReplicationMerkleTreeNode) sync;
        int partitionId = node.getPartitionId();
        int remainingEventCount = counterMap.get(partitionId)
                                            .addAndGet(-node.getEntryCount());
        if (remainingEventCount == 0) {
            syncManager.incrementSyncedPartitionCount();
        }
    }


    /**
     * Processes the WAN merkle tree sync event.
     *
     * @param event WAN merkle tree sync event
     * @throws Exception if there was an exception when waiting for the results
     *                   of the merkle tree roots
     */
    @Override
    public void processEvent(WanSyncEvent event, WanAntiEntropyEventResult result) throws Exception {
        if (event.getType() == WanSyncType.ALL_MAPS) {
            for (String mapName : mapService.getMapServiceContext().getMapContainers().keySet()) {
                lastConsistencyCheckResults.put(mapName, new ConsistencyCheckResult(-1, -1));
                processMapSync(event, result, mapName);
            }
        } else {
            String mapName = event.getMapName();
            lastConsistencyCheckResults.put(mapName, new ConsistencyCheckResult(-1, -1));
            processMapSync(event, result, mapName);
        }
    }

    private void processMapSync(WanSyncEvent event, WanAntiEntropyEventResult result, String mapName) throws Exception {
        String target = publisher.wanReplicationName + "/" + publisher.wanPublisherId;
        if (logger.isFineEnabled()) {
            logger.fine("Synchronizing map " + mapName + " to cluster " + target + " by using Merkle trees");
        }

        ConsistencyCheckResult checkResult = new ConsistencyCheckResult();
        try {
            if (logger.isFineEnabled()) {
                logger.fine("Comparing Merkle trees of map " + mapName + " with cluster " + target
                        + " to identify the difference");
            }

            List<Integer> localPartitionsToSync = getLocalPartitions(event);
            Map<Integer, int[]> diff = compareMerkleTrees(mapName, localPartitionsToSync);
            Set<Integer> processedPartitions = result.getProcessedPartitions();
            if (diff == null || diff.isEmpty()) {
                if (logger.isFineEnabled()) {
                    logger.fine("Map " + mapName + " found to be consistent with cluster " + target
                            + ", no synchronization is needed");
                }
                return;
            }
            checkResult = new ConsistencyCheckResult(localPartitionsToSync.size(), diff.size());
            if (logger.isFineEnabled()) {
                logger.fine("Merkle tree comparison for map " + mapName + " with cluster " + target + " has completed: " + diff
                        .size() + " partitions out of " + localPartitionsToSync.size() + " need to be synced");
            }

            syncDifferences(mapName, diff, processedPartitions);
            checkResult = new ConsistencyCheckResult(localPartitionsToSync.size(), 0);

            if (logger.isFineEnabled()) {
                logger.fine("Synchronization of map " + mapName + " to cluster " + target + " has finished");
            }

        } finally {
            lastConsistencyCheckResults.put(mapName, checkResult);
        }
    }

    private void syncDifferences(String mapName, Map<Integer, int[]> diff, Set<Integer> processedPartitions) {
        MerkleTreeWanSyncStats stats = new MerkleTreeWanSyncStats();

        for (Entry<Integer, int[]> partitionDiffsEntry : diff.entrySet()) {
            stats.onSyncPartition();
            Integer partitionId = partitionDiffsEntry.getKey();
            counterMap.put(partitionId, new AtomicInteger());

            int[] merkleTreeNodeOrderValuePairs = partitionDiffsEntry.getValue();
            MerkleTreeGetEntriesOperation op = new MerkleTreeGetEntriesOperation(
                    mapName, merkleTreeNodeOrderValuePairs);
            InternalCompletableFuture<Collection<MerkleTreeNodeEntries>> future =
                    nodeEngine.getOperationService()
                              .invokeOnPartition(MapService.SERVICE_NAME, op, partitionId);
            Collection<MerkleTreeNodeEntries> partitionEntries = future.join();

            for (MerkleTreeNodeEntries nodeEntries : partitionEntries) {
                if (!nodeEntries.getNodeEntries().isEmpty()) {
                    EnterpriseMapReplicationMerkleTreeNode node =
                            new EnterpriseMapReplicationMerkleTreeNode(mapName, nodeEntries, partitionId);
                    publisher.offerToStagingQueue(new WanReplicationEvent(MapService.SERVICE_NAME, node));
                    counterMap.get(partitionId).addAndGet(node.getEntryCount());
                    stats.onSyncLeaf(node.getEntryCount());
                }
            }
            processedPartitions.add(partitionId);
        }

        stats.onSyncComplete();
        logSyncStatsIfEnabled(stats);
        lastSyncStats.put(mapName, stats);
    }

    private void logSyncStatsIfEnabled(MerkleTreeWanSyncStats stats) {
        if (logger.isFineEnabled()) {
            String syncStatsMsg = String.format("Synchronization finished%n%n"
                            + "Merkle synchronization statistics:%n"
                            + "\t Duration: %d secs%n"
                            + "\t Total records synchronized: %d%n"
                            + "\t Total partitions synchronized: %d%n"
                            + "\t Total Merkle tree nodes synchronized: %d%n"
                            + "\t Average records per Merkle tree node: %.2f%n"
                            + "\t StdDev of records per Merkle tree node: %.2f%n"
                            + "\t Minimum records per Merkle tree node: %d%n"
                            + "\t Maximum records per Merkle tree node: %d%n",
                    stats.getDurationSecs(), stats.getRecordsSynced(), stats.getPartitionsSynced(), stats.getNodesSynced(),
                    stats.getAvgEntriesPerLeaf(), stats.getStdDevEntriesPerLeaf(), stats.getMinLeafEntryCount(),
                    stats.getMaxLeafEntryCount());
            logger.fine(syncStatsMsg);
        }
    }

    /**
     * Compares the local merkle trees with the remote cluster merkle trees for
     * the provided {@code mapName} and {@code partitionIds}.
     * The return value is a map from partition ID to merkle tree node order-value
     * pairs which are different.
     *
     * @param mapName      the map which should be compared
     * @param partitionIds the partition IDs which should be compared
     * @return the map of different merkle tree node order-value pairs, grouped
     * by partition ID
     * @throws Exception if there was an exception while waiting for the results
     *                   of the partition invocations
     */
    @SuppressWarnings("checkstyle:npathcomplexity")
    private Map<Integer, int[]> compareMerkleTrees(String mapName, List<Integer> partitionIds) throws Exception {
        if (partitionIds.isEmpty()) {
            return null;
        }

        Map<Integer, int[]> diff = MapUtil.createHashMap(partitionIds.size());
        for (Integer partitionId : partitionIds) {
            diff.put(partitionId, new int[0]);
        }
        MerkleTreeComparisonProcessor processor = new MerkleTreeComparisonProcessor();

        while (true) {
            Map<Integer, int[]> localNodeValues = invokeLocal(mapName, diff);
            processor.processLocalNodeValues(localNodeValues);
            if (processor.isComparisonFinished()) {
                return processor.getDifference();
            }
            diff = localNodeValues;

            Map<Integer, int[]> remoteNodeValues = compareWithRemoteCluster(mapName, diff);
            processor.processRemoteNodeValues(remoteNodeValues);
            if (processor.isComparisonFinished()) {
                return processor.getDifference();
            }
            diff = remoteNodeValues;
        }
    }

    private Map<Integer, int[]> invokeLocal(String mapName, Map<Integer, int[]> diff)
            throws Exception {
        InternalOperationService operationService = nodeEngine.getOperationService();
        MerkleTreeNodeCompareOperationFactory factory = new MerkleTreeNodeCompareOperationFactory(mapName,
                new MerkleTreeNodeValueComparison(diff));

        Set<Integer> differentPartitionIds = diff.keySet();
        return operationService.invokeOnPartitions(MapService.SERVICE_NAME, factory, differentPartitionIds);
    }

    private Map<Integer, int[]> removeIdenticalPartitions(Map<Integer, int[]> diff) {
        Iterator<Entry<Integer, int[]>> iterator = diff.entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<Integer, int[]> entry = iterator.next();
            int[] value = entry.getValue();

            if (value != null && value.length == 0) {
                iterator.remove();
            }
        }

        return diff;
    }

    /**
     * Sends the provided map of merkle tree node order-value pairs for the given
     * {@code mapName} to the target cluster and returns the result of the
     * comparison.
     *
     * @param mapName            name of the map to compare
     * @param pairsByPartitionId the local map of merkle tree node order-value pairs, grouped by
     *                           partition ID
     * @return the result of the comparison
     */
    private Map<Integer, int[]> compareWithRemoteCluster(String mapName,
                                                         Map<Integer, int[]> pairsByPartitionId) {
        List<Address> liveEndpoints = publisher.getConnectionManager().awaitAndGetTargetEndpoints();
        if (liveEndpoints.isEmpty()) {
            // this means the connection manager is shutting down
            return null;
        }

        Integer randomPartitionId = pairsByPartitionId.keySet().iterator().next();
        Address randomTarget = liveEndpoints.get(randomPartitionId % liveEndpoints.size());
        MerkleTreeNodeValueComparison comparison = new MerkleTreeNodeValueComparison(pairsByPartitionId);
        WanMerkleTreeNodeCompareOperation compareOp = new WanMerkleTreeNodeCompareOperation(mapName, comparison);

        MerkleTreeNodeValueComparison comparisonResult = invokeOnWanTarget(randomTarget, compareOp);
        Map<Integer, int[]> comparisonResultMap = new HashMap<Integer, int[]>(comparisonResult.getPartitionIds().size());
        for (int partitionId : comparisonResult.getPartitionIds()) {
            comparisonResultMap.put(partitionId, comparisonResult.getMerkleTreeNodeValues(partitionId));
        }

        return removeIdenticalPartitions(comparisonResultMap);
    }

    @Override
    public Map<String, ConsistencyCheckResult> getLastConsistencyCheckResults() {
        return lastConsistencyCheckResults;
    }

    @Override
    public Map<String, WanSyncStats> getLastSyncStats() {
        return lastSyncStats;
    }

    @Override
    public void destroyMapData(String mapName) {
        lastConsistencyCheckResults.remove(mapName);
        lastSyncStats.remove(mapName);
    }

    /**
     * Returns the intersection between local partitions and partitions required
     * by the provided WAN anti-entropy event.
     *
     * @param event the WAN anti-entropy event
     * @return the local partitions to process
     */
    private List<Integer> getLocalPartitions(WanAntiEntropyEvent event) {
        Set<Integer> partitionsToProcess = event.getPartitionSet();
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        LinkedList<Integer> localPartitionsToCheck = new LinkedList<Integer>();

        if (isEmpty(partitionsToProcess)) {
            for (IPartition partition : partitionService.getPartitions()) {
                if (partition.isLocal()) {
                    localPartitionsToCheck.add(partition.getPartitionId());
                }
            }
        } else {
            for (Integer partitionId : partitionsToProcess) {
                IPartition partition = partitionService.getPartition(partitionId);
                if (partition.isLocal()) {
                    localPartitionsToCheck.add(partition.getPartitionId());
                }
            }
        }
        return localPartitionsToCheck;
    }

    private <T> T invokeOnWanTarget(Address target, Operation operation) {
        // this method runs on a cached thread: we should guarantee that we release the thread after some time
        // the first call to this method may take longer since the connection to a remote cluster member needs to be established
        long deadline = calculateDeadline();
        int idleStep = 0;
        while (System.nanoTime() < deadline || idleStep < WAN_TARGET_INVOCATION_MIN_ATTEMPTS) {
            WanConnectionWrapper connectionWrapper = publisher.getConnectionManager().getConnection(target);
            if (connectionWrapper != null) {
                String serviceName = EnterpriseWanReplicationService.SERVICE_NAME;
                InternalOperationService operationService = nodeEngine.getOperationService();
                InternalCompletableFuture<T> future = operationService
                        .createInvocationBuilder(serviceName, operation, connectionWrapper.getConnection().getEndPoint())
                        .setTryCount(1)
                                .setCallTimeout(configurationContext.getResponseTimeoutMillis())
                                .invoke();
                return future.join();
            }

            wanTargetInvocationIdleStrategy.idle(idleStep++);
        }

        throw new IllegalStateException("Could not obtain a connection to " + target
                + " within " + WAN_TARGET_INVOCATION_DEADLINE_SECONDS + " seconds after " + idleStep + " attempts");
    }

    private long calculateDeadline() {
        return System.nanoTime() + SECONDS.toNanos(WAN_TARGET_INVOCATION_DEADLINE_SECONDS);
    }
}
