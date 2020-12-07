package com.hazelcast.enterprise.wan.impl;

import com.hazelcast.auditlog.AuditlogTypeIds;
import com.hazelcast.config.AbstractWanPublisherConfig;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.MerkleTreeConfig;
import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.config.WanBatchPublisherConfig;
import com.hazelcast.config.WanCustomPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.enterprise.wan.impl.operation.AddWanConfigOperation;
import com.hazelcast.enterprise.wan.impl.operation.AddWanConfigOperationFactory;
import com.hazelcast.enterprise.wan.impl.operation.PostJoinWanOperation;
import com.hazelcast.enterprise.wan.impl.operation.WanEventContainerOperation;
import com.hazelcast.enterprise.wan.impl.replication.WanEventBatch;
import com.hazelcast.enterprise.wan.impl.sync.WanSyncManager;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.management.events.AddWanConfigIgnoredEvent;
import com.hazelcast.internal.management.events.Event;
import com.hazelcast.internal.management.events.WanConfigurationAddedEvent;
import com.hazelcast.internal.management.events.WanConfigurationExtendedEvent;
import com.hazelcast.internal.management.events.WanConsistencyCheckIgnoredEvent;
import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.monitor.LocalWanPublisherStats;
import com.hazelcast.internal.monitor.LocalWanStats;
import com.hazelcast.internal.monitor.WanSyncState;
import com.hazelcast.internal.monitor.impl.LocalWanPublisherStatsImpl;
import com.hazelcast.internal.monitor.impl.LocalWanStatsImpl;
import com.hazelcast.internal.partition.FragmentedMigrationAwareService;
import com.hazelcast.internal.partition.PartitionMigrationEvent;
import com.hazelcast.internal.partition.PartitionReplicationEvent;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.internal.services.PostJoinAwareService;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.internal.util.InvocationUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.LiveOperations;
import com.hazelcast.spi.impl.operationservice.LiveOperationsTracker;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.version.Version;
import com.hazelcast.wan.WanConsumer;
import com.hazelcast.wan.WanEvent;
import com.hazelcast.wan.WanEventCounters;
import com.hazelcast.wan.WanEventCounters.DistributedObjectWanEventCounters;
import com.hazelcast.wan.WanPublisher;
import com.hazelcast.wan.impl.AddWanConfigResult;
import com.hazelcast.wan.impl.ConsistencyCheckResult;
import com.hazelcast.wan.impl.DelegatingWanScheme;
import com.hazelcast.wan.impl.InternalWanEvent;
import com.hazelcast.wan.impl.InternalWanPublisher;
import com.hazelcast.wan.impl.WanEventCounterRegistry;
import com.hazelcast.wan.impl.WanReplicationService;
import com.hazelcast.wan.impl.WanReplicationServiceImpl;
import com.hazelcast.wan.impl.WanSyncStats;
import com.hazelcast.wan.impl.WanSyncType;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.WAN_DISCRIMINATOR_REPLICATION;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.WAN_PREFIX;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.WAN_PREFIX_CONSISTENCY_CHECK;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.WAN_PREFIX_SYNC;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.WAN_TAG_CACHE;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.WAN_TAG_MAP;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.WAN_TAG_PUBLISHERID;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static com.hazelcast.spi.properties.ClusterProperty.WAN_CONSUMER_INVOCATION_THRESHOLD;
import static com.hazelcast.wan.impl.WanReplicationServiceImpl.getWanPublisherId;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

/**
 * Enterprise implementation for WAN replication.
 */
@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classfanoutcomplexity", "checkstyle:classdataabstractioncoupling"})
public class EnterpriseWanReplicationService implements WanReplicationService, FragmentedMigrationAwareService,
                                                        PostJoinAwareService, LiveOperationsTracker, ManagedService,
                                                        DynamicMetricsProvider {

    /**
     * Collection of supported WAN protocol versions. In the future, this may
     * be retrieved dynamically e.g. using a service loader.
     */
    private static final List<Version> SUPPORTED_WAN_PROTOCOL_VERSIONS = Collections.singletonList(Version.of(1, 0));

    private static final int ADD_WAN_CONFIG_MAX_RETRIES = 10;

    private final Node node;
    private final ILogger logger;
    private final WanMigrationAwareService migrationAwareService;
    private final WanEventProcessor eventProcessor;
    private final WanSchemeContainer publisherContainer;
    private final WanConsumerContainer consumerContainer;
    private final WanSyncManager syncManager;
    /**
     * Mutex for adding new WAN replication config
     */
    private final Object configUpdateMutex = new Object();

    /**
     * WAN event counters for all services and only received events
     */
    private final WanEventCounterRegistry receivedWanEventCounters = new WanEventCounterRegistry();

    /**
     * WAN event counters for all services and only sent events
     */
    private final WanEventCounterRegistry sentWanEventCounters = new WanEventCounterRegistry();

    private final WanAcknowledger acknowledger;

    public EnterpriseWanReplicationService(Node node) {
        this.node = node;
        this.logger = node.getLogger(EnterpriseWanReplicationService.class.getName());
        this.migrationAwareService = new WanMigrationAwareService(this, node);
        this.acknowledger = createAcknowledger();
        this.eventProcessor = new WanEventProcessor(node, acknowledger);
        this.publisherContainer = new WanSchemeContainer(node);
        this.consumerContainer = new WanConsumerContainer(node);
        this.syncManager = new WanSyncManager(this, node);
    }

    private WanAcknowledger createAcknowledger() {
        HazelcastProperties properties = node.getProperties();
        int invocationThreshold = properties.getInteger(WAN_CONSUMER_INVOCATION_THRESHOLD);
        if (invocationThreshold <= 0) {
            return new WanNonThrottlingAcknowledger(node);
        } else {
            return new WanThrottlingAcknowledger(node, invocationThreshold);
        }
    }

    /**
     * Returns a WAN replication configured under a WAN replication config with
     * the name {@code wanReplicationName} and with a WAN publisher ID of
     * {@code wanPublisherId} or {@code null} if there is no configuration for
     * the given parameters.
     *
     * @param wanReplicationName the name of the {@link WanReplicationConfig}
     * @param wanPublisherId     WAN replication publisher ID
     * @return the WAN publisher or {@code null} if there is no configuration for the
     * given parameters
     * @see WanReplicationConfig#getName
     * @see WanBatchPublisherConfig#getClusterName()
     * @see AbstractWanPublisherConfig#getPublisherId()
     */
    public WanPublisher getPublisherOrNull(String wanReplicationName,
                                           String wanPublisherId) {
        DelegatingWanScheme publisherDelegate = getWanReplicationPublishers(wanReplicationName);
        return publisherDelegate != null
                ? publisherDelegate.getPublisher(wanPublisherId)
                : null;
    }

    @Override
    public WanPublisher getPublisherOrFail(String wanReplicationName,
                                           String wanPublisherId) {
        WanPublisher publisher = getPublisherOrNull(wanReplicationName, wanPublisherId);
        if (publisher == null) {
            throw new InvalidConfigurationException("WAN Replication Config doesn't exist with WAN configuration name "
                    + wanReplicationName + " and publisher ID " + wanPublisherId);
        }
        return publisher;
    }


    /**
     * Processes the replication event sent from the source cluster.
     *
     * @param event        the event, can be of type
     *                     {@link WanEvent} or
     *                     {@link WanEventBatch}
     * @param wanOperation the operation sent by the source cluster
     */
    public void handleEvent(IdentifiedDataSerializable event, WanEventContainerOperation wanOperation) {
        if (event instanceof WanEventBatch) {
            eventProcessor.handleRepEvent((WanEventBatch) event, wanOperation);
        } else {
            eventProcessor.handleRepEvent((InternalWanEvent) event, wanOperation);
        }
    }

    @Override
    public void appendWanReplicationConfig(WanReplicationConfig newConfig) {
        String scheme = newConfig.getName();
        ConcurrentMap<String, WanReplicationConfig> wanConfigs =
                (ConcurrentMap<String, WanReplicationConfig>) node.getConfig().getWanReplicationConfigs();
        WanReplicationConfig existingConfig = wanConfigs.putIfAbsent(scheme, newConfig);

        if (existingConfig == null) {
            logger.info("Added new WAN replication configuration " + newConfig);
            return;
        }

        Map<String, WanBatchPublisherConfig> missingBatchPublishers = removeExistingPublishers(
                getPublisherConfigMap(newConfig.getBatchPublisherConfigs()),
                existingConfig.getBatchPublisherConfigs());

        Map<String, WanCustomPublisherConfig> missingCustomPublishers = removeExistingPublishers(
                getPublisherConfigMap(newConfig.getCustomPublisherConfigs()),
                existingConfig.getCustomPublisherConfigs());

        if (missingBatchPublishers.isEmpty() && missingCustomPublishers.isEmpty()) {
            return;
        }

        synchronized (configUpdateMutex) {
            existingConfig = wanConfigs.get(scheme);
            removeExistingPublishers(missingBatchPublishers, existingConfig.getBatchPublisherConfigs());
            removeExistingPublishers(missingCustomPublishers, existingConfig.getCustomPublisherConfigs());

            if (missingBatchPublishers.isEmpty() && missingCustomPublishers.isEmpty()) {
                // no new publishers
                return;
            }

            // new publishers, create new config
            WanReplicationConfig mergedConfig = new WanReplicationConfig();
            mergedConfig.setConsumerConfig(existingConfig.getConsumerConfig());
            mergedConfig.setName(existingConfig.getName());
            mergedConfig.getBatchPublisherConfigs().addAll(existingConfig.getBatchPublisherConfigs());
            mergedConfig.getBatchPublisherConfigs().addAll(missingBatchPublishers.values());

            mergedConfig.getCustomPublisherConfigs().addAll(existingConfig.getCustomPublisherConfigs());
            mergedConfig.getCustomPublisherConfigs().addAll(missingCustomPublishers.values());

            wanConfigs.put(scheme, mergedConfig);
            logger.info("Added new WAN publisher configurations "
                    + missingBatchPublishers.values() + " to WAN replication scheme: " + scheme);
        }
    }

    private <T extends AbstractWanPublisherConfig> Map<String, T> removeExistingPublishers(Map<String, T> toRemoveFrom,
                                                                                           Collection<T> toRemove) {

        for (AbstractWanPublisherConfig c : toRemove) {
            toRemoveFrom.remove(getWanPublisherId(c));
        }
        return toRemoveFrom;
    }

    void emitManagementCenterEvent(Event event) {
        // management center service may be temporarily null
        // while node is joining the cluster
        if (node.getManagementCenterService() != null) {
            node.getManagementCenterService().log(event);
        }
    }

    /**
     * Returns WAN publisher configurations grouped by publisher ID.
     *
     * @param publisherConfigs WAN publisher configurations
     * @return WAN publisher configurations grouped by publisher ID
     */
    private static <T extends AbstractWanPublisherConfig> Map<String, T> getPublisherConfigMap(Collection<T> publisherConfigs) {
        return publisherConfigs.stream()
                               .collect(toMap(WanReplicationServiceImpl::getWanPublisherId, identity()));
    }

    /**
     * Publishes an anti-entropy event for the given {@code wanReplicationName}
     * and {@code wanPublisherId}.
     * This method does not wait for the event processing to complete.
     * Silently skips publishers not supporting anti-entropy event publication.
     *
     * @param wanReplicationName the WAN replication config name
     * @param wanPublisherId     WAN replication publisher ID
     * @param event              the WAN anti-entropy event
     * @throws InvalidConfigurationException if there is no replication config
     *                                       with the {@code wanReplicationName}
     *                                       and {@code wanPublisherId}
     */
    public void publishAntiEntropyEvent(String wanReplicationName,
                                        String wanPublisherId,
                                        AbstractWanAntiEntropyEvent event) {
        WanPublisher publisher = getPublisherOrFail(wanReplicationName, wanPublisherId);
        if (publisher instanceof InternalWanPublisher) {
            syncManager.setActiveWanReplicationName(wanReplicationName);
            syncManager.setActivePublisherId(wanPublisherId);
            ((InternalWanPublisher) publisher).publishAntiEntropyEvent(event);
        }
    }

    /**
     * Returns the manager for WAN sync events.
     */
    public WanSyncManager getSyncManager() {
        return syncManager;
    }

    /**
     * Constructs and initializes all WAN consumers defined in the WAN
     * configuration
     *
     * @see WanConsumer
     */
    public void initializeCustomConsumers() {
        consumerContainer.initializeCustomConsumers();
    }

    /**
     * Returns a map of publisher delegates grouped by WAN replication config
     * name
     */
    ConcurrentHashMap<String, DelegatingWanScheme> getWanReplications() {
        return publisherContainer.getWanReplications();
    }

    // only for testing
    public void handleEvent(InternalWanEvent event, WanAcknowledgeType acknowledgeType) {
        eventProcessor.handleEvent(event, acknowledgeType);
    }

    @Override
    public DelegatingWanScheme getWanReplicationPublishers(String name) {
        return publisherContainer.getWanReplicationPublishers(name);
    }

    /**
     * {@inheritDoc}
     * Returns map from WAN replication config name to {@link LocalWanStats}.
     */
    @Override
    public Map<String, LocalWanStats> getStats() {
        ConcurrentHashMap<String, DelegatingWanScheme> wanReplications = getWanReplications();
        Map<String, LocalWanStats> wanStats = createHashMap(wanReplications.size());

        // add stats from initialized WAN replications
        for (Map.Entry<String, DelegatingWanScheme> delegateEntry : wanReplications.entrySet()) {
            LocalWanStats localWanStats = new LocalWanStatsImpl();
            String wanReplicationConfigName = delegateEntry.getKey();
            DelegatingWanScheme delegate = delegateEntry.getValue();
            localWanStats.getLocalWanPublisherStats().putAll(delegate.getStats());
            wanStats.put(wanReplicationConfigName, localWanStats);
        }

        // add stats from uninitialized but configured WAN replications
        final Map<String, WanReplicationConfig> wanReplicationConfigs
                = node.getNodeEngine().getConfig().getWanReplicationConfigs();
        for (Entry<String, WanReplicationConfig> replicationEntry : wanReplicationConfigs.entrySet()) {
            String wanReplicationConfigName = replicationEntry.getKey();
            if (wanStats.containsKey(wanReplicationConfigName)) {
                continue;
            }
            LocalWanStatsImpl localWanStats = new LocalWanStatsImpl();
            Map<String, LocalWanPublisherStats> publisherStats = localWanStats.getLocalWanPublisherStats();

            WanReplicationConfig replicationConfig = replicationEntry.getValue();
            for (WanBatchPublisherConfig config : replicationConfig.getBatchPublisherConfigs()) {
                final String publisherId = WanReplicationServiceImpl.getWanPublisherId(config);
                final LocalWanPublisherStatsImpl stats = new LocalWanPublisherStatsImpl();
                stats.setState(config.getInitialPublisherState());
                publisherStats.put(publisherId, stats);
            }

            wanStats.put(wanReplicationConfigName, localWanStats);
        }

        return wanStats;
    }

    @Override
    public WanSyncState getWanSyncState() {
        return syncManager.getWanSyncState();
    }

    @Override
    public WanEventCounters getReceivedEventCounters(String serviceName) {
        return receivedWanEventCounters.getWanEventCounter("", "", serviceName);
    }

    @Override
    public WanEventCounters getSentEventCounters(String wanReplicationName,
                                                 String wanPublisherId,
                                                 String serviceName) {
        return sentWanEventCounters.getWanEventCounter(wanReplicationName, wanPublisherId, serviceName);
    }

    @Override
    public void removeWanEventCounters(String serviceName, String dataStructureName) {
        receivedWanEventCounters.removeCounter(serviceName, dataStructureName);
        sentWanEventCounters.removeCounter(serviceName, dataStructureName);
    }

    @Override
    public List<Version> getSupportedWanProtocolVersions() {
        return SUPPORTED_WAN_PROTOCOL_VERSIONS;
    }

    @Override
    public Operation getPostJoinOperation() {
        Map<String, WanReplicationConfig> wanConfigs = node.getConfig().getWanReplicationConfigs();
        return new PostJoinWanOperation(wanConfigs.values());
    }

    @Override
    public void populate(LiveOperations liveOperations) {
        final Collection<DelegatingWanScheme> publishers = getWanReplications().values();
        // populate for all WanAntiEntropyEventPublishOperation
        for (DelegatingWanScheme delegate : publishers) {
            for (WanPublisher publisher : delegate.getPublishers()) {
                if (publisher instanceof LiveOperationsTracker) {
                    ((LiveOperationsTracker) publisher).populate(liveOperations);
                }
            }
        }
        eventProcessor.populate(liveOperations);
    }

    @Override
    public void shutdown() {
        consumerContainer.shutdown();
        syncManager.shutdown();
        eventProcessor.shutdown();
        publisherContainer.shutdown();
    }

    @Override
    public void pause(String wanReplicationName, String wanPublisherId) {
        WanPublisher publisher = getPublisherOrFail(wanReplicationName, wanPublisherId);
        if (publisher instanceof InternalWanPublisher) {
            ((InternalWanPublisher) publisher).pause();
        }
    }

    @Override
    public void stop(String wanReplicationName, String wanPublisherId) {
        WanPublisher publisher = getPublisherOrFail(wanReplicationName, wanPublisherId);
        if (publisher instanceof InternalWanPublisher) {
            ((InternalWanPublisher) publisher).stop();
        }
    }

    @Override
    public void resume(String wanReplicationName, String wanPublisherId) {
        WanPublisher publisher = getPublisherOrFail(wanReplicationName, wanPublisherId);
        if (publisher instanceof InternalWanPublisher) {
            ((InternalWanPublisher) publisher).resume();
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws com.hazelcast.enterprise.wan.impl.sync.SyncFailedException if there is a anti-entropy request in
     *                                                                    progress
     */
    @Override
    public UUID syncMap(String wanReplicationName, String wanPublisherId, String mapName) {
        node.getNodeExtension().getAuditlogService().eventBuilder(AuditlogTypeIds.WAN_SYNC)
            .message("Explicit request for WAN map synchronization")
            .addParameter("replicationName", wanReplicationName)
            .addParameter("publisherId", wanPublisherId)
            .addParameter("mapName", mapName)
            .log();
        WanSyncEvent event = new WanSyncEvent(WanSyncType.SINGLE_MAP, mapName);
        syncManager.initiateAntiEntropyRequest(wanReplicationName, wanPublisherId, event);
        return event.getUuid();
    }

    /**
     * {@inheritDoc}
     *
     * @throws com.hazelcast.enterprise.wan.impl.sync.SyncFailedException if there is a anti-entropy request in progress
     */
    @Override
    public UUID syncAllMaps(String wanReplicationName, String wanPublisherId) {
        node.getNodeExtension().getAuditlogService().eventBuilder(AuditlogTypeIds.WAN_SYNC)
            .message("Explicit request for WAN all maps synchronization")
            .addParameter("replicationName", wanReplicationName)
            .addParameter("publisherId", wanPublisherId)
            .log();
        WanSyncEvent event = new WanSyncEvent(WanSyncType.ALL_MAPS);
        syncManager.initiateAntiEntropyRequest(wanReplicationName, wanPublisherId, event);
        return event.getUuid();
    }

    /**
     * {@inheritDoc}
     *
     * @throws com.hazelcast.enterprise.wan.impl.sync.SyncFailedException if there is a anti-entropy request in progress
     */
    @Override
    public UUID consistencyCheck(String wanReplicationName, String wanPublisherId, String mapName) {
        MerkleTreeConfig merkleTreeConfig = node.getConfig().findMapConfig(mapName).getMerkleTreeConfig();
        if (!merkleTreeConfig.isEnabled()) {
            emitManagementCenterEvent(new WanConsistencyCheckIgnoredEvent(wanReplicationName,
                    wanPublisherId, mapName, "Map has merkle trees disabled."));
            logger.info(String.format(
                    "Consistency check request for WAN replication '%s',"
                            + " publisher ID '%s' and map '%s'"
                            + " ignored because map has merkle trees disabled",
                    wanReplicationName, wanPublisherId, mapName));
            return null;
        }

        WanConsistencyCheckEvent event = new WanConsistencyCheckEvent(mapName);
        syncManager.initiateAntiEntropyRequest(wanReplicationName, wanPublisherId, event);
        return event.getUuid();
    }

    @Override
    public void removeWanEvents(String wanReplicationName, String wanPublisherId) {
        WanPublisher publisher = getPublisherOrFail(wanReplicationName, wanPublisherId);
        if (publisher instanceof InternalWanPublisher) {
            ((InternalWanPublisher) publisher).removeWanEvents();
        }
    }

    @Override
    public void addWanReplicationConfigLocally(WanReplicationConfig wanReplicationConfig) {
        appendWanReplicationConfig(wanReplicationConfig);
        publisherContainer.ensurePublishersInitialized(wanReplicationConfig.getName());
    }

    @Override
    public AddWanConfigResult addWanReplicationConfig(WanReplicationConfig wanReplicationConfig) {
        Event mcEvent;
        WanReplicationConfig existingConfig = node.getConfig().getWanReplicationConfig(wanReplicationConfig.getName());
        AddWanConfigResult result;

        Set<String> newPublisherIds =
                Stream.of(wanReplicationConfig.getBatchPublisherConfigs(), wanReplicationConfig.getCustomPublisherConfigs())
                      .flatMap(Collection::stream)
                      .map(WanReplicationServiceImpl::getWanPublisherId)
                      .collect(Collectors.toSet());

        node.getNodeExtension().getAuditlogService().eventBuilder(AuditlogTypeIds.WAN_ADD_CONFIG)
            .message("WAN Adding configuration")
            .addParameter("newConfig", wanReplicationConfig)
            .addParameter("existingConfig", existingConfig)
            .log();

        if (existingConfig != null) {
            Set<String> ignoredPublisherIds =
                    Stream.of(existingConfig.getBatchPublisherConfigs(), existingConfig.getCustomPublisherConfigs())
                          .flatMap(Collection::stream)
                          .map(WanReplicationServiceImpl::getWanPublisherId)
                          .filter(newPublisherIds::contains)
                          .collect(Collectors.toSet());
            newPublisherIds.removeAll(ignoredPublisherIds);
            result = new AddWanConfigResult(newPublisherIds, ignoredPublisherIds);
            if (newPublisherIds.isEmpty()) {
                mcEvent = AddWanConfigIgnoredEvent.alreadyExists(wanReplicationConfig.getName());
            } else {
                mcEvent = new WanConfigurationExtendedEvent(wanReplicationConfig.getName(), newPublisherIds);
            }
        } else {
            mcEvent = new WanConfigurationAddedEvent(wanReplicationConfig.getName());
            result = new AddWanConfigResult(newPublisherIds, Collections.emptySet());
        }

        invokeAddWanReplicationConfig(wanReplicationConfig);
        emitManagementCenterEvent(mcEvent);
        return result;
    }

    private void invokeAddWanReplicationConfig(final WanReplicationConfig wanReplicationConfig) {
        try {
            OperationService operationService = node.getNodeEngine().getOperationService();
            operationService.invokeOnAllPartitions(null, new AddWanConfigOperationFactory(wanReplicationConfig));

            if (node.getClusterService().getClusterVersion().isUnknownOrLessThan(Versions.V4_1)) {
                // best-effort to add config locally if member does not own any
                // partitions
                addWanReplicationConfigLocally(wanReplicationConfig);
            } else {
                // adds on other members such as lite members and members
                InvocationUtil.invokeOnStableClusterSerial(node.getNodeEngine(),
                        () -> new AddWanConfigOperation(wanReplicationConfig, false), ADD_WAN_CONFIG_MAX_RETRIES).get();
            }
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    @Override
    public Collection<ServiceNamespace> getAllServiceNamespaces(PartitionReplicationEvent event) {
        return migrationAwareService.getAllServiceNamespaces(event);
    }

    @Override
    public boolean isKnownServiceNamespace(ServiceNamespace namespace) {
        return migrationAwareService.isKnownServiceNamespace(namespace);
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event,
                                                 Collection<ServiceNamespace> namespaces) {
        return migrationAwareService.prepareReplicationOperation(event, namespaces);
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        return migrationAwareService.prepareReplicationOperation(event);
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {
        migrationAwareService.beforeMigration(event);
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        migrationAwareService.commitMigration(event);
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        migrationAwareService.rollbackMigration(event);
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        ((NodeEngineImpl) nodeEngine).getMetricsRegistry().registerDynamicMetricsProvider(this);
    }

    @Override
    public void reset() {
        final Collection<DelegatingWanScheme> publishers = getWanReplications().values();
        for (DelegatingWanScheme publisherDelegate : publishers) {
            for (WanPublisher publisher : publisherDelegate.getPublishers()) {
                publisher.reset();
            }
        }
    }

    @Override
    public void shutdown(boolean terminate) {
        reset();
    }

    @Override
    public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
        Map<String, LocalWanStats> stats = getStats();
        if (stats == null) {
            return;
        }

        descriptor.withPrefix(WAN_PREFIX);

        for (Map.Entry<String, ? extends LocalWanStats> entry : stats.entrySet()) {
            String replicationName = entry.getKey();
            LocalWanStats localWanStats = entry.getValue();

            MetricDescriptor rootDescriptor = descriptor
                    .copy()
                    .withDiscriminator(WAN_DISCRIMINATOR_REPLICATION, replicationName);

            for (Entry<String, LocalWanPublisherStats> publisherEntry : localWanStats.getLocalWanPublisherStats().entrySet()) {
                String publisherId = publisherEntry.getKey();
                LocalWanPublisherStats wanPublisherStats = publisherEntry.getValue();

                // replication
                MetricDescriptor publisherDescriptor = rootDescriptor
                        .copy()
                        .withTag(WAN_TAG_PUBLISHERID, publisherId);
                context.collect(publisherDescriptor, wanPublisherStats);

                // map counter
                provideCounterMetrics(context, publisherDescriptor, wanPublisherStats.getSentMapEventCounter(), WAN_TAG_MAP);

                // cache counter
                provideCounterMetrics(context, publisherDescriptor, wanPublisherStats.getSentCacheEventCounter(), WAN_TAG_CACHE);

                // sync
                provideSyncMetrics(context, wanPublisherStats, publisherDescriptor);

                // consistency check
                provideConsistencyCheckMetrics(context, wanPublisherStats, publisherDescriptor);
            }
        }

        // WAN acknowledger
        if (acknowledger instanceof WanThrottlingAcknowledger) {
            ((WanThrottlingAcknowledger) acknowledger).provideMetrics(descriptor.copy(), context);
        }
    }

    private void provideConsistencyCheckMetrics(MetricsCollectionContext context, LocalWanPublisherStats wanPublisherStats,
                                                MetricDescriptor publisherDescriptor) {
        Map<String, ConsistencyCheckResult> lastConsistencyCheckResults = wanPublisherStats
                .getLastConsistencyCheckResults();
        if (lastConsistencyCheckResults != null) {
            for (Entry<String, ConsistencyCheckResult> syncStatsEntry : lastConsistencyCheckResults.entrySet()) {
                String mapName = syncStatsEntry.getKey();
                ConsistencyCheckResult consistencyCheckResult = syncStatsEntry.getValue();

                MetricDescriptor counterDescriptor = publisherDescriptor
                        .copy()
                        .withPrefix(WAN_PREFIX_CONSISTENCY_CHECK)
                        .withTag(WAN_TAG_MAP, mapName);
                context.collect(counterDescriptor, consistencyCheckResult);
            }
        }
    }

    private void provideSyncMetrics(MetricsCollectionContext context, LocalWanPublisherStats wanPublisherStats,
                                    MetricDescriptor publisherDescriptor) {
        Map<String, WanSyncStats> lastSyncStats = wanPublisherStats.getLastSyncStats();
        if (lastSyncStats != null) {
            for (Entry<String, WanSyncStats> syncStatsEntry : lastSyncStats.entrySet()) {
                String mapName = syncStatsEntry.getKey();
                WanSyncStats syncStats = syncStatsEntry.getValue();

                MetricDescriptor counterDescriptor = publisherDescriptor
                        .copy()
                        .withPrefix(WAN_PREFIX_SYNC)
                        .withTag(WAN_TAG_MAP, mapName);
                context.collect(counterDescriptor, syncStats);
            }
        }
    }

    private void provideCounterMetrics(MetricsCollectionContext context, MetricDescriptor publisherDescriptor,
                                       Map<String, DistributedObjectWanEventCounters> sentDsEventCounter,
                                       String dataStructure) {
        if (sentDsEventCounter == null) {
            return;
        }

        for (Entry<String, DistributedObjectWanEventCounters> sentCounterStats : sentDsEventCounter.entrySet()) {
            String dataStructureName = sentCounterStats.getKey();
            DistributedObjectWanEventCounters counterStats = sentCounterStats.getValue();

            MetricDescriptor counterDescriptor = publisherDescriptor
                .copy()
                .withTag(dataStructure, dataStructureName);
            context.collect(counterDescriptor, counterStats);
        }
    }
}
