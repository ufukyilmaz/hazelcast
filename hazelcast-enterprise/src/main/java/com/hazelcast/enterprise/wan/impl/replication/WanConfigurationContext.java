package com.hazelcast.enterprise.wan.impl.replication;

import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.config.WanBatchReplicationPublisherConfig;

import static com.hazelcast.internal.util.StringUtil.isNullOrEmpty;

/**
 * WAN configuration context providing eager parsing of configuration.
 * This context is valid for a single WAN publisher.
 */
public class WanConfigurationContext {
    private final boolean snapshotEnabled;
    private final int batchSize;
    private final long batchMaxDelayMillis;
    private final long responseTimeoutMillis;
    private final WanAcknowledgeType acknowledgeType;
    private final boolean useEndpointPrivateAddress;
    private final String groupName;
    private final int maxEndpoints;
    private final int discoveryPeriodSeconds;
    private final String endpoints;
    private final WanBatchReplicationPublisherConfig publisherConfig;
    private final int maxConcurrentInvocations;
    private final long idleMinParkNs;
    private final long idleMaxParkNs;

    WanConfigurationContext(WanBatchReplicationPublisherConfig publisherConfig) {
        this.publisherConfig = publisherConfig;
        this.snapshotEnabled = publisherConfig.isSnapshotEnabled();
        this.batchSize = publisherConfig.getBatchSize();
        this.batchMaxDelayMillis = publisherConfig.getBatchMaxDelayMillis();
        this.responseTimeoutMillis = publisherConfig.getResponseTimeoutMillis();
        this.acknowledgeType = publisherConfig.getAcknowledgeType();
        this.groupName = publisherConfig.getGroupName();
        this.useEndpointPrivateAddress = publisherConfig.isUseEndpointPrivateAddress();
        this.discoveryPeriodSeconds = publisherConfig.getDiscoveryPeriodSeconds();
        this.endpoints = publisherConfig.getTargetEndpoints();
        this.maxEndpoints = isNullOrEmpty(publisherConfig.getTargetEndpoints())
                ? publisherConfig.getMaxTargetEndpoints()
                : Integer.MAX_VALUE;
        this.maxConcurrentInvocations = publisherConfig.getMaxConcurrentInvocations();
        this.idleMinParkNs = publisherConfig.getIdleMinParkNs();
        this.idleMaxParkNs = publisherConfig.getIdleMaxParkNs();
    }

    /**
     * Retuns {@code true} if key-based coalescing is configured for this WAN
     * publisher.
     * When enabled, only the latest {@link com.hazelcast.wan.WanReplicationEvent}
     * of a key is sent to target.
     *
     * @see WanBatchReplicationPublisherConfig#isSnapshotEnabled()
     */
    public boolean isSnapshotEnabled() {
        return snapshotEnabled;
    }

    /**
     * Returns the maximum batch size that can be sent to target cluster.
     *
     * @see WanBatchReplicationPublisherConfig#getBatchSize()
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * Returns the maximum amount of time to be waited before sending a batch of
     * events to target cluster, if {@link #getBatchSize()} of events have not
     * arrived within this duration.
     *
     * @see WanBatchReplicationPublisherConfig#getBatchMaxDelayMillis()
     */
    public long getBatchMaxDelayMillis() {
        return batchMaxDelayMillis;
    }

    /**
     * Returns the duration in milliseconds to define waiting time before
     * retrying to send the events to target cluster again in case of
     * acknowledgement is not arrived.
     *
     * @see WanBatchReplicationPublisherConfig#getResponseTimeoutMillis()
     */
    public long getResponseTimeoutMillis() {
        return responseTimeoutMillis;
    }

    /**
     * Returns the acknowledgement waiting type of WAN replication operation
     * invocation.
     *
     * @see WanBatchReplicationPublisherConfig#getAcknowledgeType()
     */
    public WanAcknowledgeType getAcknowledgeType() {
        return acknowledgeType;
    }

    /**
     * Returns {@code true} if the WAN connection manager should connect to the
     * endpoint on the private address returned by the discovery SPI.
     *
     * @see WanBatchReplicationPublisherConfig#isUseEndpointPrivateAddress()
     */
    public boolean isUseEndpointPrivateAddress() {
        return useEndpointPrivateAddress;
    }

    /**
     * Returns the group name of target cluster.
     *
     * @see WanBatchReplicationPublisherConfig#getGroupName()
     */
    public String getGroupName() {
        return groupName;
    }

    /**
     * Returns the maximum number of endpoints that WAN will connect to when
     * using a discovery mechanism to define endpoints.
     *
     * @see WanBatchReplicationPublisherConfig#getMaxTargetEndpoints()
     */
    public int getMaxEndpoints() {
        return maxEndpoints;
    }

    /**
     * Returns the period in seconds in which WAN tries to discover new endpoints
     * and reestablish connections to failed endpoints.
     *
     * @see WanBatchReplicationPublisherConfig#getDiscoveryConfig()
     */
    public int getDiscoveryPeriodSeconds() {
        return discoveryPeriodSeconds;
    }

    /**
     * Returns the comma separated list of target cluster members,
     * e.g. {@code 127.0.0.1:5701, 127.0.0.1:5702}.
     *
     * @see WanBatchReplicationPublisherConfig#getTargetEndpoints()
     */
    public String getEndpoints() {
        return endpoints;
    }

    /**
     * Returns the configuration for the WAN publisher for which this context
     * is valid.
     *
     * @return WAN publisher configuration
     */
    public WanBatchReplicationPublisherConfig getPublisherConfig() {
        return publisherConfig;
    }


    /**
     * Returns the maximum number of WAN event batches being sent to the target
     * cluster concurrently. A value of less than {@code 2} means only one batch
     * will be sent per target endpoint at any point in time.
     *
     * @return the maximum number of concurrent WAN batches
     * @see WanBatchReplicationPublisherConfig#getMaxConcurrentInvocations()
     */
    public int getMaxConcurrentInvocations() {
        return maxConcurrentInvocations;
    }

    /**
     * Returns the minimum amount of time in nanoseconds that the WAN
     * replication thread will idle if there are no events to be replicated.
     *
     * @return the minimum idle time
     * @see WanBatchReplicationPublisherConfig#getIdleMinParkNs()
     */
    public long getIdleMinParkNs() {
        return idleMinParkNs;
    }

    /**
     * Returns the maximum amount of time in nanoseconds that the WAN
     * replication thread will idle if there are no events to be replicated.
     *
     * @return the maximum idle time
     * @see WanBatchReplicationPublisherConfig#getIdleMaxParkNs()
     */
    public long getIdleMaxParkNs() {
        return idleMaxParkNs;
    }
}
