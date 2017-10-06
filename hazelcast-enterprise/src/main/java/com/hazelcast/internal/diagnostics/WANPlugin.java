package com.hazelcast.internal.diagnostics;

import com.hazelcast.monitor.LocalWanPublisherStats;
import com.hazelcast.monitor.LocalWanStats;
import com.hazelcast.monitor.WanSyncState;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;
import com.hazelcast.wan.WanReplicationService;

import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.internal.diagnostics.Diagnostics.PREFIX;
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


    private final long periodMillis;
    private final WanReplicationService wanService;

    public WANPlugin(NodeEngineImpl nodeEngine) {
        super(nodeEngine.getLogger(WANPlugin.class));
        final HazelcastProperties props = nodeEngine.getProperties();

        wanService = nodeEngine.getService(WanReplicationService.SERVICE_NAME);
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
        writer.startSection("WAN");

        renderWanSyncState(writer);
        final Map<String, LocalWanStats> stats = wanService.getStats();
        for (Entry<String, LocalWanStats> entry : stats.entrySet()) {
            renderWanReplication(writer, entry.getKey(), entry.getValue());
        }

        writer.endSection();
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
        writer.writeKeyValueEntry("outboundQueueSize", stats.getOutboundQueueSize());
        writer.writeKeyValueEntry("totalPublishedEventCount", stats.getTotalPublishedEventCount());
        writer.writeKeyValueEntry("totalPublishLatency", stats.getTotalPublishLatency());
        writer.writeKeyValueEntry("connected", stats.isConnected());
        writer.writeKeyValueEntry("paused", stats.isPaused());
        writer.endSection();
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

        writer.startSection("syncState");
        writer.writeKeyValueEntry("status", syncState.getStatus().toString());
        writer.writeKeyValueEntry("activeWanConfigName", syncState.getActiveWanConfigName());
        writer.writeKeyValueEntry("activePublisherName", syncState.getActivePublisherName());
        writer.writeKeyValueEntry("syncedPartitionCount", syncState.getSyncedPartitionCount());
        writer.endSection();
    }
}
