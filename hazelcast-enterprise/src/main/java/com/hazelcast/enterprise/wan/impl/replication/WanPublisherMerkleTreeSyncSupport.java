package com.hazelcast.enterprise.wan.impl.replication;

import com.hazelcast.enterprise.wan.impl.WanConsistencyCheckEvent;
import com.hazelcast.enterprise.wan.impl.WanSyncEvent;
import com.hazelcast.enterprise.wan.impl.WanSyncType;
import com.hazelcast.enterprise.wan.impl.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.impl.connection.WanConnectionWrapper;
import com.hazelcast.enterprise.wan.impl.operation.MerkleTreeNodeValueComparison;
import com.hazelcast.enterprise.wan.impl.operation.WanMerkleTreeNodeCompareOperation;
import com.hazelcast.enterprise.wan.impl.sync.WanSyncManager;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.management.events.WanConsistencyCheckFinishedEvent;
import com.hazelcast.internal.management.events.WanConsistencyCheckStartedEvent;
import com.hazelcast.internal.management.events.WanMerkleSyncFinishedEvent;
import com.hazelcast.internal.management.events.WanSyncProgressUpdateEvent;
import com.hazelcast.internal.management.events.WanSyncStartedEvent;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MerkleTreeNodeEntries;
import com.hazelcast.map.impl.operation.MerkleTreeGetEntriesOperation;
import com.hazelcast.map.impl.operation.MerkleTreeGetEntryCountOperation;
import com.hazelcast.map.impl.operation.MerkleTreeNodeCompareOperationFactory;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationMerkleTreeNode;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationObject;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.partition.IPartition;
import com.hazelcast.internal.util.MapUtil;
import com.hazelcast.internal.util.ThreadUtil;
import com.hazelcast.internal.util.concurrent.BackoffIdleStrategy;
import com.hazelcast.internal.util.concurrent.IdleStrategy;
import com.hazelcast.wan.ConsistencyCheckResult;
import com.hazelcast.wan.WanAntiEntropyEvent;
import com.hazelcast.wan.WanSyncStats;
import com.hazelcast.wan.impl.merkletree.MerkleTreeUtil;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import static com.hazelcast.internal.util.CollectionUtil.isEmpty;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableMap;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Support class for processing WAN merkle tree anti-entropy events for a
 * single publisher.
 */
@SuppressWarnings({"checkstyle:classfanoutcomplexity", "checkstyle:classdataabstractioncoupling"})
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
            new ConcurrentHashMap<>();
    private final Map<String, WanSyncStats> lastSyncStats = new ConcurrentHashMap<>();
    private final WanBatchReplication publisher;
    private final WanSyncManager syncManager;
    private final Map<UUID, WanSyncContext<MerkleTreeWanSyncStats>> syncContextMap = new ConcurrentHashMap<>();

    /**
     * {@link IdleStrategy} used for
     */
    private final IdleStrategy wanTargetInvocationIdleStrategy;
    private final ExecutorService updateSerializingExecutor;

    WanPublisherMerkleTreeSyncSupport(Node node,
                                      WanConfigurationContext configurationContext,
                                      WanBatchReplication publisher) {
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
        this.updateSerializingExecutor = newSingleThreadExecutor(
                r -> new Thread(r, ThreadUtil.createThreadName(node.hazelcastInstance.getName(), "wan-sync-stats-updater")));
    }

    /**
     * {@inheritDoc}
     * Processes the WAN merkle tree root check event and updates the
     * {@code result} with the processing results.
     *
     * @param event WAN merkle tree root check event
     * @throws Exception if there was an exception when waiting for the results
     *                   of the merkle tree roots
     */
    @Override
    public void processEvent(WanConsistencyCheckEvent event) throws Exception {
        String mapName = event.getObjectName();
        if (!isMapWanReplicated(mapName)) {
            throw new IllegalArgumentException("WAN consistency check requested for map " + mapName + " that is "
                    + "not configured for WAN replication");
        }

        String target = publisher.wanReplicationName + "/" + publisher.wanPublisherId;
        nodeEngine.getManagementCenterService()
                  .log(new WanConsistencyCheckStartedEvent(event.getUuid(), publisher.wanReplicationName,
                          publisher.wanPublisherId, mapName));
        if (logger.isFineEnabled()) {
            logger.fine("Checking via Merkle trees if map " + mapName + " is consistent with cluster " + target);
        }
        lastConsistencyCheckResults.put(mapName, new ConsistencyCheckResult(event.getUuid(), -1, -1, -1, -1, -1));
        ConsistencyCheckResult checkResult = new ConsistencyCheckResult(event.getUuid());
        try {
            List<Integer> localPartitionsToSync = getLocalPartitions(event);
            Map<Integer, int[]> diff = compareMerkleTrees(mapName, localPartitionsToSync);
            if (diff != null) {
                int totalLocalMerkleTreeLeaves = getMerkleTreeLeaves(diff) * localPartitionsToSync.size();
                checkResult = new ConsistencyCheckResult(event.getUuid(), localPartitionsToSync.size(), diff.size(),
                        totalLocalMerkleTreeLeaves, getDiffLeafCount(diff), getEntriesToSync(mapName, diff));
                event.getProcessingResult()
                     .addProcessedPartitions(localPartitionsToSync);
            }
        } finally {
            lastConsistencyCheckResults.put(mapName, checkResult);
        }

        int entriesToSync = checkResult.getLastEntriesToSync();
        int checkedCount = checkResult.getLastCheckedPartitionCount();
        int diffCount = checkResult.getLastDiffPartitionCount();
        nodeEngine.getManagementCenterService()
                  .log(new WanConsistencyCheckFinishedEvent(event.getUuid(), publisher.wanReplicationName,
                          publisher.wanPublisherId, mapName, diffCount, checkedCount, entriesToSync));
        if (logger.isFineEnabled()) {
            logger.fine("Consistency check for map " + mapName + " with cluster " + target + " has completed: "
                    + diffCount + " partitions out of " + checkedCount + " are not consistent, " + entriesToSync
                    + " entries need to be synchronized.");
        }
    }

    private int getDiffLeafCount(Map<Integer, int[]> diff) {
        int diffLeafCount = 0;

        for (int[] pairs : diff.values()) {
            diffLeafCount += pairs.length / 2;
        }

        return diffLeafCount;
    }

    private int getMerkleTreeLeaves(Map<Integer, int[]> diff) {
        Iterator<int[]> iterator = diff.values().iterator();
        if (!iterator.hasNext()) {
            return 0;
        }

        int[] pairs = iterator.next();
        int level = MerkleTreeUtil.getLevelOfNode(pairs[0]);
        return MerkleTreeUtil.getNodesOnLevel(level);
    }

    private int getEntriesToSync(String mapName, Map<Integer, int[]> diff) {
        int entryCount = 0;
        for (Entry<Integer, int[]> partitionDiffsEntry : diff.entrySet()) {
            Integer partitionId = partitionDiffsEntry.getKey();
            int[] merkleTreeNodeOrderValuePairs = partitionDiffsEntry.getValue();
            int[] merkleTreeNodeOrders = new int[merkleTreeNodeOrderValuePairs.length / 2];
            for (int i = 0; i < merkleTreeNodeOrders.length; i++) {
                merkleTreeNodeOrders[i] = merkleTreeNodeOrderValuePairs[i * 2];
            }

            MerkleTreeGetEntryCountOperation op = new MerkleTreeGetEntryCountOperation(mapName, merkleTreeNodeOrders);
            InternalCompletableFuture<Integer> future =
                    nodeEngine.getOperationService()
                              .invokeOnPartition(MapService.SERVICE_NAME, op, partitionId);
            Integer count = future.joinInternal();
            entryCount += count;
        }

        return entryCount;
    }

    @Override
    public void removeReplicationEvent(EnterpriseMapReplicationObject sync) {
        EnterpriseMapReplicationMerkleTreeNode node = (EnterpriseMapReplicationMerkleTreeNode) sync;
        WanSyncContext<MerkleTreeWanSyncStats> syncContext = syncContextMap.get(node.getUuid());
        int partitionId = node.getPartitionId();
        int nodeEntryCount = node.getEntryCount();
        String mapName = sync.getMapName();
        int remainingEventCount = syncContext.getSyncCounter(mapName, partitionId).addAndGet(-nodeEntryCount);
        MerkleTreeWanSyncStats syncStats = syncContext.getSyncStats(mapName);

        updateSerializingExecutor.execute(() -> {
            syncStats.onSyncLeaf(nodeEntryCount);

            if (remainingEventCount == 0) {
                syncManager.incrementSyncedPartitionCount();
                syncStats.onSyncPartition();

                writeManagementCenterProgressUpdateEvent(syncContext.getUuid(), mapName, syncStats.getPartitionsSynced(),
                        syncStats, syncStats.getRecordsSynced());

                completeSyncContext(syncContext, mapName, syncStats);
            }
        });
    }

    private void completeSyncContext(WanSyncContext<MerkleTreeWanSyncStats> syncContext, String mapName,
                                     MerkleTreeWanSyncStats syncStats) {
        if (syncStats.getPartitionsToSync() == syncStats.getPartitionsSynced()) {
            syncContext.onMapSynced();
            syncStats.onSyncComplete();
            logSyncStats(syncStats);
            writeManagementCenterSyncFinishedEvent(syncContext.getUuid(), mapName, syncStats);
            cleanupSyncContextMap();
        }
    }

    private void cleanupSyncContextMap() {
        for (Map.Entry<UUID, WanSyncContext<MerkleTreeWanSyncStats>> entry : syncContextMap.entrySet()) {
            UUID key = entry.getKey();
            WanSyncContext<MerkleTreeWanSyncStats> context = entry.getValue();
            if (context.isCompletedOrStuck()) {
                syncContextMap.remove(key);
            }
        }
    }

    /**
     * {@inheritDoc}
     * Processes the WAN merkle tree sync event.
     *
     * @param event WAN merkle tree sync event
     * @throws Exception if there was an exception when waiting for the results
     *                   of the merkle tree roots
     */
    @Override
    public void processEvent(WanSyncEvent event) throws Exception {
        List<Integer> localPartitionsToSync = getLocalPartitions(event);
        UUID uuid = event.getUuid();
        if (event.getType() == WanSyncType.ALL_MAPS) {
            List<String> mapNames = new LinkedList<>();

            mapService.getMapServiceContext().getMapContainers().keySet().forEach(mapName -> {
                if (isMapWanReplicated(mapName)) {
                    mapNames.add(mapName);
                }
            });

            if (!isEmpty(mapNames)) {
                WanSyncContext<MerkleTreeWanSyncStats> syncContext = new WanSyncContext<>(uuid, localPartitionsToSync.size(),
                        mapNames);
                syncContextMap.put(uuid, syncContext);
                for (String mapName : mapNames) {
                    lastConsistencyCheckResults.put(mapName, new ConsistencyCheckResult(uuid, -1, -1, -1, -1, -1));
                    processMapSync(event, syncContext, mapName, localPartitionsToSync);
                }
            }
        } else {
            String mapName = event.getObjectName();
            if (!isMapWanReplicated(mapName)) {
                throw new IllegalArgumentException("WAN synchronization requested for map " + mapName + " that is "
                        + "not configured for WAN replication");
            }
            lastConsistencyCheckResults.put(mapName, new ConsistencyCheckResult(uuid, -1, -1, -1, -1, -1));
            WanSyncContext<MerkleTreeWanSyncStats> syncContext = new WanSyncContext<>(uuid, localPartitionsToSync.size(),
                    singletonList(mapName));
            syncContextMap.put(uuid, syncContext);
            processMapSync(event, syncContext, mapName, localPartitionsToSync);
        }
    }

    private boolean isMapWanReplicated(String mapName) {
        return mapService.getMapServiceContext().getMapContainer(mapName).isWanReplicationEnabled();
    }

    private void processMapSync(WanSyncEvent event, WanSyncContext<MerkleTreeWanSyncStats> syncContext, String mapName,
                                List<Integer> localPartitionsToSync) throws Exception {
        String target = publisher.wanReplicationName + "/" + publisher.wanPublisherId;
        if (logger.isFineEnabled()) {
            logger.fine("Synchronizing map " + mapName + " to cluster " + target + " by using Merkle trees");
        }

        UUID uuid = event.getUuid();
        ConsistencyCheckResult checkResult = new ConsistencyCheckResult(uuid);
        try {
            nodeEngine.getManagementCenterService()
                      .log(new WanConsistencyCheckStartedEvent(uuid, publisher.wanReplicationName,
                              publisher.wanPublisherId, mapName));
            if (logger.isFineEnabled()) {
                logger.fine("Comparing Merkle trees of map " + mapName + " with cluster " + target
                        + " to identify the difference");
            }

            Map<Integer, int[]> diff = compareMerkleTrees(mapName, localPartitionsToSync);
            Set<Integer> processedPartitions = event.getProcessingResult()
                                                    .getProcessedPartitions();
            if (diff == null || diff.isEmpty()) {
                MerkleTreeWanSyncStats stats = new MerkleTreeWanSyncStats(syncContext.getUuid(), 0);
                syncContext.addSyncStats(mapName, stats);
                lastSyncStats.put(mapName, stats);

                if (logger.isFineEnabled()) {
                    logger.fine("Map " + mapName + " found to be consistent with cluster " + target
                            + ", no synchronization is needed");
                }
                nodeEngine.getManagementCenterService()
                          .log(new WanConsistencyCheckFinishedEvent(uuid, publisher.wanReplicationName,
                                  publisher.wanPublisherId, mapName, 0, localPartitionsToSync.size(), 0));

                // we don't sync anything, but update the stats, log and send the events to MC for tracking purpose
                nodeEngine.getManagementCenterService()
                          .log(new WanSyncStartedEvent(uuid, publisher.wanReplicationName, publisher.wanPublisherId, mapName));

                completeSyncContext(syncContext, mapName, syncContext.getSyncStats(mapName));
                return;
            }
            int entriesToSync = getEntriesToSync(mapName, diff);
            int totalLocalMerkleTreeLeaves = getMerkleTreeLeaves(diff) * localPartitionsToSync.size();
            checkResult = new ConsistencyCheckResult(uuid, localPartitionsToSync.size(), diff.size(), totalLocalMerkleTreeLeaves,
                    getDiffLeafCount(diff), entriesToSync);
            nodeEngine.getManagementCenterService()
                      .log(new WanConsistencyCheckFinishedEvent(uuid, publisher.wanReplicationName, publisher.wanPublisherId,
                              mapName, diff.size(), localPartitionsToSync.size(), entriesToSync));
            if (logger.isFineEnabled()) {
                logger.fine("Merkle tree comparison for map " + mapName + " with cluster " + target + " has completed: " + diff
                        .size() + " partitions out of " + localPartitionsToSync.size() + " need to be synced");
            }

            nodeEngine.getManagementCenterService()
                      .log(new WanSyncStartedEvent(uuid, publisher.wanReplicationName, publisher.wanPublisherId, mapName));

            syncDifferences(syncContext, mapName, diff, processedPartitions);

        } finally {
            lastConsistencyCheckResults.put(mapName, checkResult);
        }
    }

    private void syncDifferences(WanSyncContext<MerkleTreeWanSyncStats> syncContext, String mapName, Map<Integer, int[]> diff,
                                 Set<Integer> processedPartitions) {
        MerkleTreeWanSyncStats stats = new MerkleTreeWanSyncStats(syncContext.getUuid(), diff.size());
        syncContext.addSyncStats(mapName, stats);
        lastSyncStats.put(mapName, stats);

        for (Entry<Integer, int[]> partitionDiffsEntry : diff.entrySet()) {
            Integer partitionId = partitionDiffsEntry.getKey();

            int[] merkleTreeNodeOrderValuePairs = partitionDiffsEntry.getValue();
            MerkleTreeGetEntriesOperation op = new MerkleTreeGetEntriesOperation(
                    mapName, merkleTreeNodeOrderValuePairs);
            InternalCompletableFuture<Collection<MerkleTreeNodeEntries>> future =
                    nodeEngine.getOperationService()
                              .invokeOnPartition(MapService.SERVICE_NAME, op, partitionId);
            Collection<MerkleTreeNodeEntries> partitionEntries = future.joinInternal();

            for (MerkleTreeNodeEntries nodeEntries : partitionEntries) {
                if (!nodeEntries.getNodeEntries().isEmpty()) {
                    EnterpriseMapReplicationMerkleTreeNode node =
                            new EnterpriseMapReplicationMerkleTreeNode(syncContext.getUuid(), mapName, nodeEntries, partitionId);
                    syncContext.getSyncCounter(mapName, partitionId).addAndGet(node.getEntryCount());
                    publisher.putToSyncEventQueue(node);
                }
            }
            processedPartitions.add(partitionId);
        }
    }

    private void logSyncStats(MerkleTreeWanSyncStats stats) {
        String syncStatsMsg = String.format("Synchronization finished%n%n"
                        + "Merkle synchronization statistics:%n"
                        + "\t Synchronization UUID: %s%n"
                        + "\t Duration: %d secs%n"
                        + "\t Total records synchronized: %d%n"
                        + "\t Total partitions synchronized: %d%n"
                        + "\t Total Merkle tree nodes synchronized: %d%n"
                        + "\t Average records per Merkle tree node: %.2f%n"
                        + "\t StdDev of records per Merkle tree node: %.2f%n"
                        + "\t Minimum records per Merkle tree node: %d%n"
                        + "\t Maximum records per Merkle tree node: %d%n",
                stats.getUuid(), stats.getDurationSecs(), stats.getRecordsSynced(), stats.getPartitionsSynced(),
                stats.getNodesSynced(), stats.getAvgEntriesPerLeaf(), stats.getStdDevEntriesPerLeaf(),
                stats.getMinLeafEntryCount(), stats.getMaxLeafEntryCount());
        logger.info(syncStatsMsg);
    }

    private void writeManagementCenterSyncFinishedEvent(UUID uuid, String mapName, MerkleTreeWanSyncStats stats) {
        WanMerkleSyncFinishedEvent event = new WanMerkleSyncFinishedEvent(uuid, publisher.wanReplicationName,
                publisher.wanPublisherId,
                mapName, stats.getDurationSecs(), stats.getPartitionsSynced(), stats.getNodesSynced(),
                stats.getRecordsSynced(), stats.getMinLeafEntryCount(), stats.getMaxLeafEntryCount(),
                stats.getAvgEntriesPerLeaf(), stats.getStdDevEntriesPerLeaf());
        nodeEngine.getManagementCenterService()
                  .log(event);
    }

    private void writeManagementCenterProgressUpdateEvent(UUID uuid, String mapName, int partitionsSynced,
                                                          MerkleTreeWanSyncStats stats, int recordsSynced) {
        WanSyncProgressUpdateEvent event = new WanSyncProgressUpdateEvent(uuid, publisher.wanReplicationName,
                publisher.wanPublisherId, mapName, stats.getPartitionsToSync(), partitionsSynced, recordsSynced);
        nodeEngine.getManagementCenterService().log(event);
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
        OperationService operationService = nodeEngine.getOperationService();
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
        Map<Integer, int[]> comparisonResultMap = new HashMap<>(comparisonResult.getPartitionIds().size());
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
        return unmodifiableMap(lastSyncStats);
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
        LinkedList<Integer> localPartitionsToCheck = new LinkedList<>();

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
                OperationService operationService = nodeEngine.getOperationService();
                InternalCompletableFuture<T> future = operationService
                        .createInvocationBuilder(serviceName, operation, connectionWrapper.getConnection().getEndPoint())
                        .setTryCount(1)
                        .setEndpointManager(connectionWrapper.getConnection().getEndpointManager())
                        .setCallTimeout(configurationContext.getResponseTimeoutMillis())
                        .invoke();
                return future.joinInternal();
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
