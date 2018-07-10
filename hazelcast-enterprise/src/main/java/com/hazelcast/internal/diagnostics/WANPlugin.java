package com.hazelcast.internal.diagnostics;

import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.monitor.LocalWanPublisherStats;
import com.hazelcast.monitor.LocalWanStats;
import com.hazelcast.monitor.WanSyncState;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;
import com.hazelcast.wan.WanReplicationService;
import com.hazelcast.wan.impl.DistributedServiceWanEventCounters;
import com.hazelcast.wan.impl.DistributedServiceWanEventCounters.DistributedObjectWanEventCounters;

import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.internal.diagnostics.Diagnostics.PREFIX;
import static com.hazelcast.util.MapUtil.isNullOrEmpty;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * The WANPlugin exposes WAN state to the diagnostics system. Currently it allows
 * inspecting the {@link WanSyncState} and {@link LocalWanPublisherStats} for each
 * {@link com.hazelcast.enterprise.wan.WanReplicationEndpoint}.
 * This allows to get an overview of the WAN system, mainly how events are being published.
 * Detailed introspection of the WAN queues is not available.
 */
public class WANPlugin extends DiagnosticsPlugin {

    /**
     * The period in seconds this plugin runs.
     * If set to 0, the plugin is disabled.
     */
    public static final HazelcastProperty PERIOD_SECONDS = new HazelcastProperty(PREFIX + ".wan.period.seconds", 0, SECONDS);

    private static final String WAN_SECTION_NAME = "WAN";

    private static final String CACHE_EVENT_COUNT_SECTION_PREFIX = "cache";
    private static final String MAP_EVENT_COUNT_SECTION_PREFIX = "map";
    private static final String SYNC_EVENT_COUNT_KEY = "syncCount";
    private static final String UPDATE_EVENT_COUNT_KEY = "updateCount";
    private static final String REMOVE_EVENT_COUNT_KEY = "removeCount";
    private static final String DROPPED_EVENT_COUNT_KEY = "droppedCount";

    private static final String PUBLISHER_OUTBOUND_QUEUE_SIZE_KEY = "outboundQueueSize";
    private static final String PUBLISHER_TOTAL_PUBLISHED_EVENT_COUNT_KEY = "totalPublishedEventCount";
    private static final String PUBLISHER_TOTAL_PUBLISH_LATENCY_KEY = "totalPublishLatency";
    private static final String PUBLISHER_CONNECTED_KEY = "connected";
    private static final String PUBLISHER_PAUSED_KEY = "paused";

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
     * {@link com.hazelcast.enterprise.wan.WanReplicationEndpoint}s.
     *
     * @param writer                   the diagnostics log writer
     * @param wanReplicationConfigName the WAN replication config name
     * @param stats                    the WAN replication statistics
     * @see com.hazelcast.enterprise.wan.WanReplicationPublisherDelegate
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
     * Renders the diagnostics for a single {@link com.hazelcast.enterprise.wan.WanReplicationEndpoint}.
     * This includes information on the current state of replication and the WAN queues
     * as well as information on events already published.
     *
     * @param writer    the diagnostics log writer
     * @param groupName the WAN replication group name
     * @param stats     the statistics for the {@link com.hazelcast.enterprise.wan.WanReplicationEndpoint}
     */
    private void renderWanPublisher(DiagnosticsLogWriter writer, String groupName, LocalWanPublisherStats stats) {
        writer.startSection(groupName);
        writer.writeKeyValueEntry(PUBLISHER_OUTBOUND_QUEUE_SIZE_KEY, stats.getOutboundQueueSize());
        writer.writeKeyValueEntry(PUBLISHER_TOTAL_PUBLISHED_EVENT_COUNT_KEY, stats.getTotalPublishedEventCount());
        writer.writeKeyValueEntry(PUBLISHER_TOTAL_PUBLISH_LATENCY_KEY, stats.getTotalPublishLatency());
        writer.writeKeyValueEntry(PUBLISHER_CONNECTED_KEY, stats.isConnected());
        writer.writeKeyValueEntry(PUBLISHER_PAUSED_KEY, stats.isPaused());
        final PublisherEventCounts publisherEventCounts = new PublisherEventCounts();
        renderSentEventCounts(writer, CACHE_EVENT_COUNT_SECTION_PREFIX,
                stats.getSentCacheEventCounter(), publisherEventCounts);
        renderSentEventCounts(writer, MAP_EVENT_COUNT_SECTION_PREFIX,
                stats.getSentMapEventCounter(), publisherEventCounts);
        writer.writeKeyValueEntry(UPDATE_EVENT_COUNT_KEY, publisherEventCounts.totalUpdateCount);
        writer.writeKeyValueEntry(REMOVE_EVENT_COUNT_KEY, publisherEventCounts.totalRemoveCount);
        writer.writeKeyValueEntry(SYNC_EVENT_COUNT_KEY, publisherEventCounts.totalSyncCount);
        writer.writeKeyValueEntry(DROPPED_EVENT_COUNT_KEY, publisherEventCounts.totalDroppedCount);
        writer.endSection();
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
     * {@link com.hazelcast.enterprise.wan.sync.WanSyncManager} such as the
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
