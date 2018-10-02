package com.hazelcast.enterprise.wan.replication;

import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.config.WanPublisherConfig;

import java.util.Map;

import static com.hazelcast.enterprise.wan.replication.WanReplicationProperties.ACK_TYPE;
import static com.hazelcast.enterprise.wan.replication.WanReplicationProperties.BATCH_MAX_DELAY_MILLIS;
import static com.hazelcast.enterprise.wan.replication.WanReplicationProperties.BATCH_SIZE;
import static com.hazelcast.enterprise.wan.replication.WanReplicationProperties.DISCOVERY_PERIOD;
import static com.hazelcast.enterprise.wan.replication.WanReplicationProperties.DISCOVERY_USE_ENDPOINT_PRIVATE_ADDRESS;
import static com.hazelcast.enterprise.wan.replication.WanReplicationProperties.ENDPOINTS;
import static com.hazelcast.enterprise.wan.replication.WanReplicationProperties.EXECUTOR_THREAD_COUNT;
import static com.hazelcast.enterprise.wan.replication.WanReplicationProperties.GROUP_PASSWORD;
import static com.hazelcast.enterprise.wan.replication.WanReplicationProperties.MAX_ENDPOINTS;
import static com.hazelcast.enterprise.wan.replication.WanReplicationProperties.RESPONSE_TIMEOUT_MILLIS;
import static com.hazelcast.enterprise.wan.replication.WanReplicationProperties.SNAPSHOT_ENABLED;
import static com.hazelcast.enterprise.wan.replication.WanReplicationProperties.getProperty;
import static com.hazelcast.util.StringUtil.isNullOrEmpty;

/**
 * WAN configuration context providing eager parsing of configuration.
 * This context is valid for a single WAN publisher.
 *
 * @see WanPublisherConfig
 */
public class WanConfigurationContext {
    /**
     * Default number of threads that the {@link WanBatchReplication} executor will have.
     *
     * @see WanReplicationProperties#EXECUTOR_THREAD_COUNT
     */
    public static final int DEFAULT_EXECUTOR_THREAD_COUNT = -1;
    /**
     * Default maximum size of a batch of events sent to the target cluster.
     * It comes into effect when used with the {@link WanBatchReplication}.
     *
     * @see WanReplicationProperties#BATCH_SIZE
     */
    public static final int DEFAULT_BATCH_SIZE = 500;

    /**
     * Default maximum amount of time to be waited before sending a batch of
     * events to target cluster, if the size constraint has not been met within
     * this duration.
     * It comes into effect when used with the {@link WanBatchReplication}.
     *
     * @see WanReplicationProperties#BATCH_MAX_DELAY_MILLIS
     */
    static final long DEFAULT_BATCH_MAX_DELAY_MILLIS = 1000;

    /**
     * Duration in milliseconds to define waiting time before retrying to
     * send the events to target cluster again in case of acknowledgement
     * is not arrived.
     * It comes into effect when used with the {@link WanBatchReplication}.
     *
     * @see WanReplicationProperties#RESPONSE_TIMEOUT_MILLIS
     */
    static final long DEFAULT_RESPONSE_TIMEOUT_MILLIS = 60000;

    /**
     * Default maximum number of endpoints to connect to. This number is
     * used if the user defines the target endpoints using the
     * {@link WanReplicationProperties#ENDPOINTS}
     * property or if there is no explicitly configured max endpoint count.
     *
     * @see WanReplicationProperties#MAX_ENDPOINTS
     */
    static final int DEFAULT_MAX_ENDPOINTS = Integer.MAX_VALUE;

    /**
     * Default period for running discovery for new endpoints in seconds
     *
     * @see com.hazelcast.enterprise.wan.connection.WanConnectionManager
     * @see WanReplicationProperties#DISCOVERY_PERIOD
     */
    static final int DEFAULT_DISCOVERY_TASK_PERIOD = 10;

    /**
     * The default group password if none is configured in the publisher
     * properties.
     *
     * @see WanReplicationProperties#GROUP_PASSWORD
     */
    static final String DEFAULT_GROUP_PASS = "dev-pass";

    /**
     * The default comma separated list of target cluster members.
     *
     * @see WanReplicationProperties#ENDPOINTS
     */
    static final String DEFAULT_ENDPOINTS = "";

    /**
     * The default value determining if the WAN connection manager should
     * connect to the endpoint on the private address returned by the discovery
     * SPI.
     *
     * @see WanReplicationProperties#DISCOVERY_USE_ENDPOINT_PRIVATE_ADDRESS
     */
    static final boolean DEFAULT_USE_ENDPOINT_PRIVATE_ADDRESS = false;

    /**
     * The default property value determining if key-based coalescing is
     * configured for this WAN publisher.
     *
     * @see WanReplicationProperties#SNAPSHOT_ENABLED
     */
    static final boolean DEFAULT_IS_SNAPSHOT_ENABLED = false;

    /**
     * The default property value determining the acknowledgement waiting type
     * of WAN replication operation invocation.
     *
     * @see WanReplicationProperties#ACK_TYPE
     */
    static final String DEFAULT_ACKNOWLEDGE_TYPE = WanAcknowledgeType.ACK_ON_OPERATION_COMPLETE.name();

    private final boolean snapshotEnabled;
    private final int executorThreadCount;
    private final int batchSize;
    private final long batchMaxDelayMillis;
    private final long responseTimeoutMillis;
    private final WanAcknowledgeType acknowledgeType;
    private final boolean useEndpointPrivateAddress;
    private final String groupName;
    private final String password;
    private final int maxEndpoints;
    private final int discoveryPeriodSeconds;
    private final String endpoints;
    private final WanPublisherConfig publisherConfig;

    WanConfigurationContext(WanPublisherConfig publisherConfig) {
        this.publisherConfig = publisherConfig;
        Map<String, Comparable> publisherProperties = publisherConfig.getProperties();
        this.snapshotEnabled = getProperty(
                SNAPSHOT_ENABLED, publisherProperties, DEFAULT_IS_SNAPSHOT_ENABLED);
        this.executorThreadCount = getProperty(
                EXECUTOR_THREAD_COUNT, publisherProperties, DEFAULT_EXECUTOR_THREAD_COUNT);
        this.batchSize = getProperty(
                BATCH_SIZE, publisherProperties, DEFAULT_BATCH_SIZE);
        this.batchMaxDelayMillis = getProperty(
                BATCH_MAX_DELAY_MILLIS, publisherProperties, DEFAULT_BATCH_MAX_DELAY_MILLIS);
        this.responseTimeoutMillis = getProperty(
                RESPONSE_TIMEOUT_MILLIS, publisherProperties, DEFAULT_RESPONSE_TIMEOUT_MILLIS);
        this.acknowledgeType = WanAcknowledgeType.valueOf(getProperty(
                ACK_TYPE, publisherProperties, DEFAULT_ACKNOWLEDGE_TYPE));
        this.groupName = publisherConfig.getGroupName();
        this.password = getProperty(
                GROUP_PASSWORD, publisherProperties, DEFAULT_GROUP_PASS);
        this.useEndpointPrivateAddress = getProperty(
                DISCOVERY_USE_ENDPOINT_PRIVATE_ADDRESS, publisherProperties, DEFAULT_USE_ENDPOINT_PRIVATE_ADDRESS);
        this.discoveryPeriodSeconds = getProperty(
                DISCOVERY_PERIOD, publisherProperties, DEFAULT_DISCOVERY_TASK_PERIOD);
        this.endpoints = getProperty(
                ENDPOINTS, publisherProperties, DEFAULT_ENDPOINTS);
        this.maxEndpoints = isNullOrEmpty(getProperty(ENDPOINTS, publisherProperties, ""))
                ? getProperty(MAX_ENDPOINTS, publisherProperties, DEFAULT_MAX_ENDPOINTS)
                : DEFAULT_MAX_ENDPOINTS;
    }

    /**
     * Retuns {@code true} if key-based coalescing is configured for this WAN
     * publisher.
     * When enabled, only the latest {@link com.hazelcast.wan.WanReplicationEvent}
     * of a key is sent to target.
     *
     * @see WanReplicationProperties#SNAPSHOT_ENABLED
     */
    public boolean isSnapshotEnabled() {
        return snapshotEnabled;
    }

    /**
     * Returns the number of threads that the {@link WanBatchReplication}
     * executor must have.
     *
     * @see WanReplicationProperties#EXECUTOR_THREAD_COUNT
     */
    public int getExecutorThreadCount() {
        return executorThreadCount;
    }

    /**
     * Returns the maximum batch size that can be sent to target cluster.
     *
     * @see WanReplicationProperties#BATCH_SIZE
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * Returns the maximum amount of time to be waited before sending a batch of
     * events to target cluster, if {@link #getBatchSize()} of events have not
     * arrived within this duration.
     *
     * @see WanReplicationProperties#BATCH_MAX_DELAY_MILLIS
     */
    public long getBatchMaxDelayMillis() {
        return batchMaxDelayMillis;
    }

    /**
     * Returns the duration in milliseconds to define waiting time before
     * retrying to send the events to target cluster again in case of
     * acknowledgement is not arrived.
     *
     * @see WanReplicationProperties#RESPONSE_TIMEOUT_MILLIS
     */
    public long getResponseTimeoutMillis() {
        return responseTimeoutMillis;
    }

    /**
     * Returns the acknowledgement waiting type of WAN replication operation
     * invocation.
     *
     * @see WanReplicationProperties#ACK_TYPE
     */
    public WanAcknowledgeType getAcknowledgeType() {
        return acknowledgeType;
    }


    /**
     * Returns {@code true} if the WAN connection manager should connect to the
     * endpoint on the private address returned by the discovery SPI.
     *
     * @see WanReplicationProperties#DISCOVERY_USE_ENDPOINT_PRIVATE_ADDRESS
     */
    public boolean isUseEndpointPrivateAddress() {
        return useEndpointPrivateAddress;
    }

    /**
     * Returns the group name of target cluster.
     *
     * @see WanPublisherConfig#getGroupName()
     */
    public String getGroupName() {
        return groupName;
    }

    /**
     * Returns the group password of target cluster.
     *
     * @see WanReplicationProperties#GROUP_PASSWORD
     */
    public String getPassword() {
        return password;
    }

    /**
     * Returns the maximum number of endpoints that WAN will connect to when
     * using a discovery mechanism to define endpoints.
     *
     * @see WanReplicationProperties#MAX_ENDPOINTS
     */
    public int getMaxEndpoints() {
        return maxEndpoints;
    }

    /**
     * Returns the period in seconds in which WAN tries to discover new endpoints
     * and reestablish connections to failed endpoints.
     *
     * @see WanReplicationProperties#DISCOVERY_PERIOD
     */
    public int getDiscoveryPeriodSeconds() {
        return discoveryPeriodSeconds;
    }

    /**
     * Returns the comma separated list of target cluster members,
     * e.g. {@code 127.0.0.1:5701, 127.0.0.1:5702}.
     *
     * @see WanReplicationProperties#ENDPOINTS
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
    public WanPublisherConfig getPublisherConfig() {
        return publisherConfig;
    }
}
