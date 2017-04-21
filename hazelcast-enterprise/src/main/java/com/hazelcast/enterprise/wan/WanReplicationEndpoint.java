package com.hazelcast.enterprise.wan;

import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.enterprise.wan.sync.WanSyncEvent;
import com.hazelcast.instance.Node;
import com.hazelcast.monitor.LocalWanPublisherStats;
import com.hazelcast.wan.WanReplicationEvent;
import com.hazelcast.wan.WanReplicationPublisher;

/**
 * Implementations of this interface represent a replication endpoint, normally another
 * Hazelcast cluster only reachable over a Wide Area Network (WAN).
 */
public interface WanReplicationEndpoint extends WanReplicationPublisher {

    /**
     * Initializes the endpoint using the given arguments.
     *
     * @param node                 the current node that tries to connect
     * @param wanReplicationConfig {@link com.hazelcast.config.WanReplicationConfig} config
     * @param wanPublisherConfig   this endpoint will be initialized using this {@link WanPublisherConfig} instance
     */
    void init(Node node, WanReplicationConfig wanReplicationConfig, WanPublisherConfig wanPublisherConfig);

    /**
     * Closes the endpoint and its internal connections and shuts down other internal states.
     */
    void shutdown();

    /**
     * Remove the oldest replication event for the same cache/map name and partition ID as {@code wanReplicationEvent} from
     * the replication queue and decreases the backup event count.
     *
     * @param wanReplicationEvent the completed wan event
     */
    void removeBackup(WanReplicationEvent wanReplicationEvent);

    void putBackup(WanReplicationEvent wanReplicationEvent);

    PublisherQueueContainer getPublisherQueueContainer();

    void addMapQueue(String key, int partitionId, WanReplicationEventQueue value);

    void addCacheQueue(String key, int partitionId, WanReplicationEventQueue value);

    /**
     * Calls to this method will pause WAN event queue polling. Effectively, pauses WAN replication for
     * its {@link WanReplicationEndpoint} instance.
     *
     * WAN events will still be offered to WAN replication
     * queues but they won't be polled.
     *
     * Calling this method on already paused {@link WanReplicationEndpoint} instances will have no effect.
     */
    void pause();

    /**
     * This method re-enables WAN event queue polling for a paused {@link WanReplicationEndpoint} instance.
     *
     * Calling this method on already running {@link WanReplicationEndpoint} instances will have no effect.
     *
     * @see #pause()
     */
    void resume();

    /**
     * Gathers statistics of related {@link WanReplicationEndpoint} instance. This method will always return the
     * same instance.
     *
     * @return {@link LocalWanPublisherStats}
     */
    LocalWanPublisherStats getStats();

    @Override
    void checkWanReplicationQueues();

    /**
     * Publishes a wan sync event for all or a specific map and for all or some partitions.
     *
     * @param syncRequest the wan sync request
     */
    void publishSyncEvent(WanSyncEvent syncRequest);

    void clearQueues();
}
