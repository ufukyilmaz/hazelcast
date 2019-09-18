package com.hazelcast.internal.diagnostics;

import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.enterprise.wan.impl.replication.MerkleTreeWanSyncStats;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.monitor.LocalWanPublisherStats;
import com.hazelcast.monitor.LocalWanStats;
import com.hazelcast.monitor.WanSyncState;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;
import com.hazelcast.wan.ConsistencyCheckResult;
import com.hazelcast.wan.DistributedServiceWanEventCounters;
import com.hazelcast.wan.DistributedServiceWanEventCounters.DistributedObjectWanEventCounters;
import com.hazelcast.wan.WanSyncStats;
import com.hazelcast.wan.impl.DelegatingWanReplicationScheme;
import com.hazelcast.wan.impl.WanReplicationService;

import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.util.MapUtil.isNullOrEmpty;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * The WANPlugin exposes WAN state to the diagnostics system. Currently it allows
 * inspecting the {@link WanSyncState} and {@link LocalWanPublisherStats} for each
 * {@link com.hazelcast.wan.WanReplicationPublisher}.
 * This allows to get an overview of the WAN system, mainly how events are being published.
 * Detailed introspection of the WAN queues is not available.
 */
public class WANPlugin extends DiagnosticsPlugin {

    /**
     * The period in seconds this plugin runs.
     * If set to 0, the plugin is disabled.
     */
    public static final HazelcastProperty PERIOD_SECONDS
            = new HazelcastProperty("hazelcast.diagnostics.wan.period.seconds", 0, SECONDS);

    private static final String WAN_SECTION_NAME = "WAN";

    private static final String UUID_KEY = "uuid";

    private static final String CACHE_EVENT_COUNT_SECTION_PREFIX = "cache";
    private static final String MAP_EVENT_COUNT_SECTION_PREFIX = "map";
    private static final String MAP_CONSISTENCY_CHECK_SECTION_PREFIX = "mapConsistencyCheck";
    private static final String MAP_SYNC_STATS_SECTION_PREFIX = "mapSyncStats";
    private static final String SYNC_EVENT_COUNT_KEY = "syncCount";
    private static final String UPDATE_EVENT_COUNT_KEY = "updateCount";
    private static final String REMOVE_EVENT_COUNT_KEY = "removeCount";
    private static final String DROPPED_EVENT_COUNT_KEY = "droppedCount";

    private static final String CONSISTENCY_CHECK_IS_RUNNING_KEY = "isRunning";
    private static final String CONSISTENCY_CHECK_LAST_CHECKED_COUNT_KEY = "checkedPartitionCount";
    private static final String CONSISTENCY_CHECK_LAST_DIFF_COUNT_KEY = "diffPartitionCount";
    private static final String CONSISTENCY_CHECK_LAST_CHECKED_LEAF_COUNT_KEY = "checkedLeafCount";
    private static final String CONSISTENCY_CHECK_LAST_DIFF_LEAF_COUNT_KEY = "diffLeafCount";
    private static final String CONSISTENCY_CHECK_ENTRIES_TO_SYNC_KEY = "entriesToSync";

    private static final String SYNC_STATS_DURATION = "durationSecs";
    private static final String SYNC_STATS_PARTITIONS = "partitionsSynced";
    private static final String SYNC_STATS_RECORDS = "recordsSynced";

    private static final String MERKLE_SYNC_STATS_NODES = "merkleNodesSynced";
    private static final String MERKLE_SYNC_STATS_AVG_PER_LEAF = "avgRecordsPerMerkleNode";
    private static final String MERKLE_SYNC_STATS_STDDEV_PER_LEAF = "stdDevRecordsPerMerkleNode";
    private static final String MERKLE_SYNC_STATS_MIN_PER_LEAF = "minRecordsPerMerkleNode";
    private static final String MERKLE_SYNC_STATS_MAX_PER_LEAF = "maxRecordsPerMerkleNode";

    private static final String PUBLISHER_OUTBOUND_QUEUE_SIZE_KEY = "outboundQueueSize";
    private static final String PUBLISHER_TOTAL_PUBLISHED_EVENT_COUNT_KEY = "totalPublishedEventCount";
    private static final String PUBLISHER_TOTAL_PUBLISH_LATENCY_KEY = "totalPublishLatency";
    private static final String PUBLISHER_CONNECTED_KEY = "connected";
    private static final String PUBLISHER_STATE_KEY = "state";

    private static final String WAN_SYNC_SECTION_NAME = "syncState";
    private static final String WAN_SYNC_STATUS_KEY = "status";
    private static final String WAN_SYNC_ACTIVE_WAN_CONFIG_NAME_KEY = "activeWanConfigName";
    private static final String WAN_SYNC_ACTIVE_PUBLISHER_NAME_KEY = "activePublisherName";
    private static final String WAN_SYNC_SYNCED_PARTITION_COUNT_KEY = "syncedPartitionCount";


    private final long periodMillis;
    private final WanReplicationService wanService;

    public WANPlugin(NodeEngineImpl nodeEngine) {
        super(nodeEngine.getLogger(WANPlugin.class));
        final HazelcastProperties props = nodeEngine.getProperties();

        this.wanService = nodeEngine.getWanReplicationService();
        this.periodMillis = props.getMillis(PERIOD_SECONDS);
    }

    @Override
    public long getPeriodMillis() {
        return periodMillis;
    }

    @Override
    public void onStart() {
        logger.info("Plugin:active, period-millis:" + periodMillis);
    }

    @Override
    public void run(DiagnosticsLogWriter writer) {
        writer.startSection(WAN_SECTION_NAME);
        final PublisherEventCounts publisherEventCounts = new PublisherEventCounts();
        renderReceivedEvents(MAP_EVENT_COUNT_SECTION_PREFIX, writer,
                wanService.getReceivedEventCounters(MapService.SERVICE_NAME), publisherEventCounts);
        renderReceivedEvents(CACHE_EVENT_COUNT_SECTION_PREFIX, writer,
                wanService.getReceivedEventCounters(CacheService.SERVICE_NAME), publisherEventCounts);
        writer.writeKeyValueEntry(SYNC_EVENT_COUNT_KEY, publisherEventCounts.totalSyncCount);
        writer.writeKeyValueEntry(UPDATE_EVENT_COUNT_KEY, publisherEventCounts.totalUpdateCount);
        writer.writeKeyValueEntry(REMOVE_EVENT_COUNT_KEY, publisherEventCounts.totalRemoveCount);

        renderWanSyncState(writer);
        final Map<String, LocalWanStats> stats = wanService.getStats();
        if (stats != null) {
            for (Entry<String, LocalWanStats> entry : stats.entrySet()) {
                renderWanReplication(writer, entry.getKey(), entry.getValue());
            }
        }
        writer.endSection();
    }

    /**
     * Renders the diagnostics for received events for all distributed
     * objects of a single type (map or cache).
     *
     * @param distributedObjectType prefix for section under which the stats are
     *                              grouped
     * @param writer                the diagnostics log writer
     * @param eventCounter          the received event counts
     */
    private void renderReceivedEvents(String distributedObjectType,
                                      DiagnosticsLogWriter writer,
                                      DistributedServiceWanEventCounters eventCounter,
                                      PublisherEventCounts publisherEventCounts) {
        if (eventCounter != null && !isNullOrEmpty(eventCounter.getEventCounterMap())) {
            for (Entry<String, DistributedObjectWanEventCounters> mapCounterEntry
                    : eventCounter.getEventCounterMap().entrySet()) {
                writer.startSection(distributedObjectType + "ReceivedEvents-" + mapCounterEntry.getKey());
                final long syncCount = mapCounterEntry.getValue().getSyncCount();
                final long updateCount = mapCounterEntry.getValue().getUpdateCount();
                final long removeCount = mapCounterEntry.getValue().getRemoveCount();
                publisherEventCounts.totalSyncCount += syncCount;
                publisherEventCounts.totalUpdateCount += updateCount;
                publisherEventCounts.totalRemoveCount += removeCount;

                writer.writeKeyValueEntry(SYNC_EVENT_COUNT_KEY, syncCount);
                writer.writeKeyValueEntry(UPDATE_EVENT_COUNT_KEY, updateCount);
                writer.writeKeyValueEntry(REMOVE_EVENT_COUNT_KEY, removeCount);
                writer.endSection();
            }
        }
    }

    /**
     * Renders the diagnostics for a single WAN replication config.
     * The config may consist of statistics for multiple
     * {@link com.hazelcast.wan.WanReplicationPublisher}s.
     *
     * @param writer                   the diagnostics log writer
     * @param wanReplicationConfigName the WAN replication config name
     * @param stats                    the WAN replication statistics
     * @see DelegatingWanReplicationScheme
     */
    private void renderWanReplication(DiagnosticsLogWriter writer, String wanReplicationConfigName, LocalWanStats stats) {
        final Map<String, LocalWanPublisherStats> publisherStats = stats.getLocalWanPublisherStats();
        writer.startSection(wanReplicationConfigName);
        for (Entry<String, LocalWanPublisherStats> publisher : publisherStats.entrySet()) {
            renderWanPublisher(writer, publisher.getKey(), publisher.getValue());
        }
        writer.endSection();
    }

    /**
     * Renders the diagnostics for a single {@link com.hazelcast.wan.WanReplicationPublisher}.
     * This includes information on the current state of replication and the WAN queues
     * as well as information on events already published.
     *
     * @param writer         the diagnostics log writer
     * @param wanPublisherId the publisher ID
     * @param stats          the statistics for the {@link com.hazelcast.wan.WanReplicationPublisher}
     */
    private void renderWanPublisher(DiagnosticsLogWriter writer, String wanPublisherId, LocalWanPublisherStats stats) {
        writer.startSection(wanPublisherId);
        writer.writeKeyValueEntry(PUBLISHER_OUTBOUND_QUEUE_SIZE_KEY, stats.getOutboundQueueSize());
        writer.writeKeyValueEntry(PUBLISHER_TOTAL_PUBLISHED_EVENT_COUNT_KEY, stats.getTotalPublishedEventCount());
        writer.writeKeyValueEntry(PUBLISHER_TOTAL_PUBLISH_LATENCY_KEY, stats.getTotalPublishLatency());
        writer.writeKeyValueEntry(PUBLISHER_CONNECTED_KEY, stats.isConnected());
        writer.writeKeyValueEntry(PUBLISHER_STATE_KEY, stats.getPublisherState().toString());
        final PublisherEventCounts publisherEventCounts = new PublisherEventCounts();
        renderSentEventCounts(writer, CACHE_EVENT_COUNT_SECTION_PREFIX,
                stats.getSentCacheEventCounter(), publisherEventCounts);
        renderSentEventCounts(writer, MAP_EVENT_COUNT_SECTION_PREFIX,
                stats.getSentMapEventCounter(), publisherEventCounts);
        writer.writeKeyValueEntry(UPDATE_EVENT_COUNT_KEY, publisherEventCounts.totalUpdateCount);
        writer.writeKeyValueEntry(REMOVE_EVENT_COUNT_KEY, publisherEventCounts.totalRemoveCount);
        writer.writeKeyValueEntry(SYNC_EVENT_COUNT_KEY, publisherEventCounts.totalSyncCount);
        writer.writeKeyValueEntry(DROPPED_EVENT_COUNT_KEY, publisherEventCounts.totalDroppedCount);
        renderConsistencyCheckResults(writer, stats.getLastConsistencyCheckResults());
        renderSyncStats(writer, stats.getLastSyncStats());
        writer.endSection();
    }

    /**
     * Writes the results of the last merkle tree root comparison for all maps.
     *
     * @param writer  the diagnostics log writer
     * @param results the merkle tree root comparison results
     */
    private void renderConsistencyCheckResults(DiagnosticsLogWriter writer,
                                               Map<String, ConsistencyCheckResult> results) {
        if (!isNullOrEmpty(results)) {
            for (Entry<String, ConsistencyCheckResult> comparisonEntry : results.entrySet()) {
                String mapName = comparisonEntry.getKey();
                ConsistencyCheckResult result = comparisonEntry.getValue();
                writer.startSection(MAP_CONSISTENCY_CHECK_SECTION_PREFIX + "-" + mapName);
                writer.writeKeyValueEntry(UUID_KEY, result.getUuid().toString());
                writer.writeKeyValueEntry(CONSISTENCY_CHECK_IS_RUNNING_KEY, result.isRunning());
                writer.writeKeyValueEntry(CONSISTENCY_CHECK_LAST_CHECKED_COUNT_KEY, result.getLastCheckedPartitionCount());
                writer.writeKeyValueEntry(CONSISTENCY_CHECK_LAST_DIFF_COUNT_KEY, result.getLastDiffPartitionCount());
                writer.writeKeyValueEntry(CONSISTENCY_CHECK_LAST_CHECKED_LEAF_COUNT_KEY, result.getLastCheckedLeafCount());
                writer.writeKeyValueEntry(CONSISTENCY_CHECK_LAST_DIFF_LEAF_COUNT_KEY, result.getLastDiffLeafCount());
                writer.writeKeyValueEntry(CONSISTENCY_CHECK_ENTRIES_TO_SYNC_KEY, result.getLastEntriesToSync());
                writer.endSection();
            }
        }
    }

    /**
     * Writes the statistics about the last synchronization for all maps.
     *
     * @param writer        the diagnostics log writer
     * @param lastSyncStats the statistics about the last synchronizations
     *                      for all maps
     */
    private void renderSyncStats(DiagnosticsLogWriter writer, Map<String, WanSyncStats> lastSyncStats) {
        if (!isNullOrEmpty(lastSyncStats)) {
            for (Entry<String, WanSyncStats> syncStatsEntry : lastSyncStats.entrySet()) {
                String mapName = syncStatsEntry.getKey();
                WanSyncStats stats = syncStatsEntry.getValue();

                writer.startSection(MAP_SYNC_STATS_SECTION_PREFIX + "-" + mapName);
                writer.writeKeyValueEntry(UUID_KEY, stats.getUuid().toString());
                writer.writeKeyValueEntry(SYNC_STATS_DURATION, stats.getDurationSecs());
                writer.writeKeyValueEntry(SYNC_STATS_PARTITIONS, stats.getPartitionsSynced());
                writer.writeKeyValueEntry(SYNC_STATS_RECORDS, stats.getRecordsSynced());

                if (stats instanceof MerkleTreeWanSyncStats) {
                    MerkleTreeWanSyncStats merkleStats = (MerkleTreeWanSyncStats) stats;
                    writer.writeKeyValueEntry(MERKLE_SYNC_STATS_NODES, merkleStats.getNodesSynced());
                    writer.writeKeyValueEntry(MERKLE_SYNC_STATS_AVG_PER_LEAF, merkleStats.getAvgEntriesPerLeaf());
                    writer.writeKeyValueEntry(MERKLE_SYNC_STATS_STDDEV_PER_LEAF, merkleStats.getStdDevEntriesPerLeaf());
                    writer.writeKeyValueEntry(MERKLE_SYNC_STATS_MIN_PER_LEAF, merkleStats.getMinLeafEntryCount());
                    writer.writeKeyValueEntry(MERKLE_SYNC_STATS_MAX_PER_LEAF, merkleStats.getMaxLeafEntryCount());
                }

                writer.endSection();
            }
        }
    }

    private void renderSentEventCounts(DiagnosticsLogWriter writer,
                                       String distributedObjectType,
                                       Map<String, DistributedObjectWanEventCounters> sentEventCount,
                                       PublisherEventCounts publisherEventCounts) {

        if (!isNullOrEmpty(sentEventCount)) {
            for (Entry<String, DistributedObjectWanEventCounters> sentCacheEvents : sentEventCount.entrySet()) {
                final DistributedObjectWanEventCounters sentEvents = sentCacheEvents.getValue();
                writer.startSection(distributedObjectType + "SentEvents-" + sentCacheEvents.getKey());
                publisherEventCounts.totalUpdateCount += sentEvents.getUpdateCount();
                publisherEventCounts.totalRemoveCount += sentEvents.getRemoveCount();
                publisherEventCounts.totalSyncCount += sentEvents.getSyncCount();
                publisherEventCounts.totalDroppedCount += sentEvents.getDroppedCount();
                writer.writeKeyValueEntry(UPDATE_EVENT_COUNT_KEY, sentEvents.getUpdateCount());
                writer.writeKeyValueEntry(REMOVE_EVENT_COUNT_KEY, sentEvents.getRemoveCount());
                writer.writeKeyValueEntry(SYNC_EVENT_COUNT_KEY, sentEvents.getSyncCount());
                writer.writeKeyValueEntry(DROPPED_EVENT_COUNT_KEY, sentEvents.getDroppedCount());
                writer.endSection();
            }
        }
    }

    /**
     * Renders the diagnostics for the state of the
     * {@link com.hazelcast.enterprise.wan.impl.sync.WanSyncManager} such as the
     * status and active sync state.
     *
     * @param writer the diagnostics log writer
     */
    private void renderWanSyncState(DiagnosticsLogWriter writer) {
        final WanSyncState syncState = wanService.getWanSyncState();

        writer.startSection(WAN_SYNC_SECTION_NAME);
        writer.writeKeyValueEntry(WAN_SYNC_STATUS_KEY, syncState.getStatus().toString());
        writer.writeKeyValueEntry(WAN_SYNC_ACTIVE_WAN_CONFIG_NAME_KEY, syncState.getActiveWanConfigName());
        writer.writeKeyValueEntry(WAN_SYNC_ACTIVE_PUBLISHER_NAME_KEY, syncState.getActivePublisherName());
        writer.writeKeyValueEntry(WAN_SYNC_SYNCED_PARTITION_COUNT_KEY, syncState.getSyncedPartitionCount());
        writer.endSection();
    }

    /**
     * Container for total event counts for a single publisher
     */
    private static class PublisherEventCounts {
        long totalUpdateCount;
        long totalRemoveCount;
        long totalSyncCount;
        long totalDroppedCount;
    }
}
