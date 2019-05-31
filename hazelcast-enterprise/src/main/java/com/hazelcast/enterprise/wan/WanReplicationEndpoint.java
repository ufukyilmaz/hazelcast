package com.hazelcast.enterprise.wan;

import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.enterprise.wan.sync.WanSyncEvent;
import com.hazelcast.instance.Node;
import com.hazelcast.monitor.LocalWanPublisherStats;
import com.hazelcast.spi.ServiceNamespace;
import com.hazelcast.spi.partition.PartitionReplicationEvent;
import com.hazelcast.wan.WanReplicationEvent;
import com.hazelcast.wan.WanReplicationPublisher;

import java.util.Collection;
import java.util.Set;

/**
 * Implementations of this interface represent a replication endpoint,
 * normally another Hazelcast cluster only reachable over a Wide Area
 * Network (WAN).
 */
public interface WanReplicationEndpoint extends WanReplicationPublisher {

    /**
     * Initializes the endpoint using the given arguments.
     *
     * @param node                 the current node that tries to connect
     * @param wanReplicationConfig the replication config
     * @param wanPublisherConfig   this endpoint will be initialized using
     *                             this {@link WanPublisherConfig} instance
     */
    void init(Node node, WanReplicationConfig wanReplicationConfig, WanPublisherConfig wanPublisherConfig);

    /**
     * Signals the publisher to shut down and clean up its resources. The
     * method does not necessarily block until the endpoint has shut down.
     */
    void shutdown();

    /**
     * Remove the oldest replication event from the replication queue and
     * decrease the backup event count. The replication queue chosen has the
     * same map/cache name and partition ID as the provided
     * {@code wanReplicationEvent}.
     *
     * @param wanReplicationEvent the completed wan event
     */
    void removeBackup(WanReplicationEvent wanReplicationEvent);

    /**
     * Puts the given {@code wanReplicationEvent} in the corresponding WAN backup
     * queue.
     *
     * @param wanReplicationEvent the WAN event
     */
    void putBackup(WanReplicationEvent wanReplicationEvent);

    /**
     * Returns the WAN event queue container for this publisher.
     */
    PublisherQueueContainer getPublisherQueueContainer();

    /**
     * Publishes all events from the {@code eventQueue} to the WAN queue for
     * the given partition ID and map name.
     *
     * @param name        the name of the map to which to publish to
     * @param partitionId the partition to which to publish to
     * @param eventQueue  the event queue from which events are published
     */
    void addMapQueue(String name, int partitionId, WanReplicationEventQueue eventQueue);

    /**
     * Publishes all events from the {@code eventQueue} to the WAN queue for
     * the given partition ID and cache name.
     *
     * @param name        the name of the cache to which to publish to
     * @param partitionId the partition to which to publish to
     * @param eventQueue  the event queue from which events are published
     */
    void addCacheQueue(String name, int partitionId, WanReplicationEventQueue eventQueue);

    /**
     * Calls to this method will pause WAN event queue polling. Effectively,
     * pauses WAN replication for its {@link WanReplicationEndpoint} instance.
     * <p>
     * WAN events will still be offered to WAN replication queues but they won't
     * be polled. This means that the queues might eventually fill up and start
     * dropping events.
     * <p>
     * Calling this method on already paused {@link WanReplicationEndpoint}
     * instances will have no effect.
     * <p></p>
     * There is no synchronization with the thread polling the WAN
     * queues and trasmitting the events to the target cluster. This means
     * that the queues may be polled even after this method returns.
     *
     * @see #resume()
     */
    void pause();

    /**
     * Calls to this method will stop WAN replication. In addition to not polling
     * events as in the {@link #pause()} method, an endpoint which is stopped
     * will not enqueue events. This method will not clear the WAN queues, though.
     * This means that once this method returns, there might still be some WAN
     * events enqueued but these events will not be replicated until the publisher
     * is resumed.
     * <p>
     * Calling this method on already stopped {@link WanReplicationEndpoint}
     * instances will have no effect.
     *
     * @see #resume()
     */
    void stop();

    /**
     * This method re-enables WAN event queue polling for a paused or stopped
     * {@link WanReplicationEndpoint} instance.
     * <p>
     * Calling this method on already running {@link WanReplicationEndpoint}
     * instances will have no effect.
     *
     * @see #pause()
     * @see #stop()
     */
    void resume();

    /**
     * Gathers statistics of related {@link WanReplicationEndpoint} instance. This method will always return the
     * same instance.
     *
     * @return {@link LocalWanPublisherStats}
     */
    LocalWanPublisherStats getStats();

    /**
     * Publishes a WAN sync event for all or a specific map and for all or
     * some partitions.
     *
     * @param syncRequest the WAN sync request
     */
    void publishSyncEvent(WanSyncEvent syncRequest);

    /**
     * Clears the WAN queues and recalculates the WAN queue sizes.
     */
    void clearQueues();

    /**
     * Collect all replication data for the specific replication event and
     * collection of namespaces being replicated.
     *
     * @param wanReplicationName     the WAN replication name in the hazelcast
     *                               configuration for this endpoint
     * @param event                  the replication event
     * @param namespaces             the object namespaces which are being
     *                               replicated
     * @param migrationDataContainer the container for the migration data
     */
    void collectReplicationData(String wanReplicationName,
                                PartitionReplicationEvent event,
                                Collection<ServiceNamespace> namespaces,
                                EWRMigrationContainer migrationDataContainer);

    /**
     * Collect the namespaces of all queues that should be replicated by the replication event.
     *
     * @param event      the replication event
     * @param namespaces the set in which namespaces should be added
     */
    void collectAllServiceNamespaces(PartitionReplicationEvent event, Set<ServiceNamespace> namespaces);
}
