package com.hazelcast.enterprise.wan;

import com.hazelcast.config.AbstractWanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.instance.impl.Node;
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
 *
 * @param <T> WAN event container type (used for replication and migration inside the
 *            cluster)
 */
public interface WanReplicationEndpoint<T> extends WanReplicationPublisher {

    /**
     * Initializes the endpoint using the given arguments.
     *
     * @param node                 the current node that tries to connect
     * @param wanReplicationConfig the replication config
     * @param wanPublisherConfig   this endpoint will be initialized using
     *                             this {@link AbstractWanPublisherConfig} instance
     */
    void init(Node node, WanReplicationConfig wanReplicationConfig, AbstractWanPublisherConfig wanPublisherConfig);

    /**
     * Signals the publisher to shut down and clean up its resources. The
     * method does not necessarily block until the endpoint has shut down.
     */
    void shutdown();

    /**
     * Calls to this method will pause WAN event container polling. Effectively,
     * pauses WAN replication for its {@link WanReplicationEndpoint} instance.
     * <p>
     * WAN events will still be offered to WAN event containers but they won't
     * be polled. This means that the containers might eventually fill up and start
     * dropping events.
     * <p>
     * Calling this method on already paused {@link WanReplicationEndpoint}
     * instances will have no effect.
     * <p></p>
     * There is no synchronization with the thread polling the WAN event
     * containers and trasmitting the events to the target cluster. This means
     * that the containers may be polled even after this method returns.
     *
     * @see #resume()
     * @see #stop()
     */
    void pause();

    /**
     * Calls to this method will stop WAN replication. In addition to not polling
     * events as in the {@link #pause()} method, an endpoint which is stopped
     * should not accept new events. This method will not remove existing events.
     * This means that once this method returns, there might still be some WAN
     * events in the containers but these events will not be replicated until
     * the publisher is resumed.
     * <p>
     * Calling this method on already stopped {@link WanReplicationEndpoint}
     * instances will have no effect.
     *
     * @see #resume()
     * @see #pause()
     */
    void stop();

    /**
     * This method re-enables WAN event containers polling for a paused or stopped
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
     * Returns a container containing the WAN events for the given replication
     * {@code event} and {@code namespaces} to be replicated. The replication
     * here refers to the intra-cluster replication between members in a single
     * cluster and does not refer to WAN replication, e.g. between two clusters.
     *
     * @param event      the replication event
     * @param namespaces namespaces which will be replicated
     * @return the WAN event container
     * @see #processEventContainerReplicationData(int, Object)
     */
    T prepareEventContainerReplicationData(PartitionReplicationEvent event,
                                           Collection<ServiceNamespace> namespaces);

    /**
     * Processes the WAN event container received through intra-cluster replication
     * or migration. This method may completely remove existing WAN events for
     * the given {@code partitionId} or it may append the given
     * {@code eventContainer} to the existing events.
     *
     * @param partitionId    partition ID which is being replicated or migrated
     * @param eventContainer the WAN event container
     * @see #prepareEventContainerReplicationData(PartitionReplicationEvent, Collection)
     */
    void processEventContainerReplicationData(int partitionId, T eventContainer);

    /**
     * Collect the namespaces of all WAN event containers that should be replicated
     * by the replication event.
     *
     * @param event      the replication event
     * @param namespaces the set in which namespaces should be added
     */
    void collectAllServiceNamespaces(PartitionReplicationEvent event, Set<ServiceNamespace> namespaces);

    /**
     * Puts the given {@code wanReplicationEvent} in the corresponding WAN backup
     * event container.
     *
     * @param wanReplicationEvent the WAN event
     */
    void putBackup(WanReplicationEvent wanReplicationEvent);

    /**
     * Removes all WAN events pending replication.
     * If the publisher does not store WAN events, this method is a no-op.
     */
    int removeWanEvents();

    /**
     * Removes all WAN events pending replication and belonging to the provided
     * service and partition.
     * If the publisher does not store WAN events, this method is a no-op.
     *
     * @param serviceName the service name of the WAN events should be removed
     * @param partitionId the partition ID of the WAN events should be removed
     */
    int removeWanEvents(int partitionId, String serviceName);

    /**
     * Removes a {@code count} number of events pending replication and belonging
     * to the provided service, object and partition.
     * If the publisher does not store WAN events, this method is a no-op.
     *
     * @param serviceName the service name of the WAN events should be removed
     * @param objectName  the object name of the WAN events should be removed
     * @param partitionId the partition ID of the WAN events should be removed
     * @param count       the number of events to remove
     */
    int removeWanEvents(int partitionId, String serviceName, String objectName, int count);
}
