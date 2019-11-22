package com.hazelcast.instance.impl;

import com.hazelcast.cache.impl.EnterpriseCacheService;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.AdvancedNetworkConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.HotRestartPersistenceConfig;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.OnJoinPermissionOperationName;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.cp.internal.persistence.CPPersistenceService;
import com.hazelcast.cp.internal.persistence.CPPersistenceServiceImpl;
import com.hazelcast.enterprise.wan.impl.EnterpriseWanReplicationService;
import com.hazelcast.hotrestart.HotRestartException;
import com.hazelcast.hotrestart.HotRestartService;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.EnterprisePhoneHome;
import com.hazelcast.internal.ascii.TextCommandService;
import com.hazelcast.internal.auditlog.AuditlogService;
import com.hazelcast.internal.auditlog.impl.ILoggerAuditlogService;
import com.hazelcast.internal.auditlog.impl.NoOpAuditlogService;
import com.hazelcast.internal.cluster.impl.ClusterVersionAutoUpgradeHelper;
import com.hazelcast.internal.cluster.impl.JoinMessage;
import com.hazelcast.internal.cluster.impl.VersionMismatchException;
import com.hazelcast.internal.diagnostics.Diagnostics;
import com.hazelcast.internal.diagnostics.WANPlugin;
import com.hazelcast.internal.dynamicconfig.DynamicConfigListener;
import com.hazelcast.internal.dynamicconfig.HotRestartConfigListener;
import com.hazelcast.internal.hotrestart.HotBackupService;
import com.hazelcast.internal.hotrestart.HotRestartIntegrationService;
import com.hazelcast.internal.hotrestart.InternalHotRestartService;
import com.hazelcast.internal.hotrestart.NoOpHotRestartService;
import com.hazelcast.internal.hotrestart.NoopInternalHotRestartService;
import com.hazelcast.internal.hotrestart.cluster.ClusterHotRestartEventListener;
import com.hazelcast.internal.hotrestart.memory.HotRestartPoolingMemoryManager;
import com.hazelcast.internal.jmx.HazelcastMBean;
import com.hazelcast.internal.jmx.ManagementService;
import com.hazelcast.internal.management.ManagementCenterConnectionFactory;
import com.hazelcast.internal.management.TimedMemberStateFactory;
import com.hazelcast.internal.memory.FreeMemoryChecker;
import com.hazelcast.internal.memory.HazelcastMemoryManager;
import com.hazelcast.internal.memory.MemoryStats;
import com.hazelcast.internal.memory.PoolingMemoryManager;
import com.hazelcast.internal.memory.StandardMemoryManager;
import com.hazelcast.internal.memory.impl.LibMallocFactory;
import com.hazelcast.internal.memory.impl.PersistentMemoryMallocFactory;
import com.hazelcast.internal.memory.impl.UnsafeMallocFactory;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.StaticMetricsProvider;
import com.hazelcast.internal.monitor.impl.jmx.EnterpriseManagementService;
import com.hazelcast.internal.monitor.impl.jmx.LicenseInfoMBean;
import com.hazelcast.internal.monitor.impl.management.EnterpriseManagementCenterConnectionFactory;
import com.hazelcast.internal.monitor.impl.management.EnterpriseTimedMemberStateFactory;
import com.hazelcast.internal.monitor.impl.rest.EnterpriseTextCommandServiceImpl;
import com.hazelcast.internal.monitor.impl.rest.LicenseInfoImpl;
import com.hazelcast.internal.networking.ChannelInitializerProvider;
import com.hazelcast.internal.networking.InboundHandler;
import com.hazelcast.internal.networking.OutboundHandler;
import com.hazelcast.internal.nio.CipherByteArrayProcessor;
import com.hazelcast.internal.nio.EnterpriseChannelInitializerProvider;
import com.hazelcast.internal.nio.IOService;
import com.hazelcast.internal.nio.tcp.SymmetricCipherPacketDecoder;
import com.hazelcast.internal.nio.tcp.SymmetricCipherPacketEncoder;
import com.hazelcast.internal.nio.tcp.TcpIpConnection;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.EnterpriseClusterVersionListener;
import com.hazelcast.internal.serialization.impl.EnterpriseSerializationServiceBuilder;
import com.hazelcast.internal.util.ByteArrayProcessor;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.internal.util.LicenseExpirationReminderTask;
import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.license.domain.Feature;
import com.hazelcast.license.domain.License;
import com.hazelcast.license.domain.LicenseVersion;
import com.hazelcast.license.exception.InvalidLicenseException;
import com.hazelcast.license.util.LicenseHelper;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.nio.MemberSocketInterceptor;
import com.hazelcast.nio.SocketInterceptor;
import com.hazelcast.partition.PartitioningStrategy;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.security.SecurityService;
import com.hazelcast.security.impl.SecurityContextImpl;
import com.hazelcast.security.impl.SecurityServiceImpl;
import com.hazelcast.security.impl.WeakSecretsConfigChecker;
import com.hazelcast.security.jsm.HazelcastRuntimePermission;
import com.hazelcast.spi.impl.executionservice.TaskScheduler;
import com.hazelcast.spi.impl.operationexecutor.impl.PartitionOperationThread;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.version.MemberVersion;
import com.hazelcast.version.Version;
import com.hazelcast.wan.impl.WanReplicationService;
import com.hazelcast.wan.impl.WanReplicationServiceImpl;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Supplier;
import java.util.logging.Level;

import static com.hazelcast.instance.EndpointQualifier.MEMBER;
import static com.hazelcast.internal.nio.CipherHelper.createSymmetricReaderCipher;
import static com.hazelcast.internal.nio.CipherHelper.createSymmetricWriterCipher;
import static com.hazelcast.internal.util.StringUtil.isNullOrEmpty;
import static com.hazelcast.map.impl.EnterpriseMapServiceConstructor.getEnterpriseMapServiceConstructor;

/**
 * This class is the enterprise system hook to allow injection of enterprise services into Hazelcast subsystems.
 */
@SuppressWarnings({
        "checkstyle:classdataabstractioncoupling",
        "checkstyle:classfanoutcomplexity",
        "checkstyle:methodcount"
})
public class EnterpriseNodeExtension
        extends DefaultNodeExtension
        implements NodeExtension, StaticMetricsProvider {

    /**
     * A map of license features removed per license version.
     */
    private static final Map<LicenseVersion, Set<Feature>> LICENSE_FEATURE_REMOVALS
            = new HashMap<>();
    /**
     * A map of new license features introduced per license version.
     */
    private static final Map<LicenseVersion, Set<Feature>> LICENSE_FEATURE_ADDITIONS
            = new HashMap<>();
    static {
        LICENSE_FEATURE_REMOVALS.put(LicenseVersion.V5, EnumSet.of(Feature.WEB_SESSION));
        LICENSE_FEATURE_ADDITIONS.put(LicenseVersion.V5, EnumSet.of(Feature.CLIENT_FILTERING, Feature.CP_PERSISTENCE));
    }

    private static final int SUGGESTED_MAX_NATIVE_MEMORY_SIZE_PER_PARTITION_IN_MB = 256;
    private static final NoopInternalHotRestartService NOOP_INTERNAL_HOT_RESTART_SERVICE = new NoopInternalHotRestartService();
    private static final NoOpHotRestartService NO_OP_HOT_RESTART_SERVICE = new NoOpHotRestartService();

    private final HotRestartIntegrationService hotRestartService;
    private final HotBackupService hotBackupService;
    private final SecurityService securityService;
    private final AuditlogService auditlogService;
    private final BuildInfo buildInfo = BuildInfoProvider.getBuildInfo();
    private final ClusterVersionAutoUpgradeHelper clusterVersionAutoUpgradeHelper
            = new ClusterVersionAutoUpgradeHelper();
    private final boolean cpPersistenceEnabled;

    private volatile CPPersistenceServiceImpl cpPersistenceService;
    private volatile License license;
    private volatile SecurityContext securityContext;
    private volatile HazelcastMemoryManager memoryManager;
    private final ConcurrentMap<EndpointQualifier, MemberSocketInterceptor> socketInterceptors
            = new ConcurrentHashMap<>();
    private volatile LicenseExpirationReminderTask licenseExpirationReminderTask;

    public EnterpriseNodeExtension(Node node) {
        super(node);
        hotRestartService = createHotRestartService(node);
        hotBackupService = createHotBackupService(node, hotRestartService);
        securityService = createSecurityService(node);
        auditlogService = createAuditlogService(node);
        cpPersistenceEnabled = node.getConfig().getCPSubsystemConfig().isPersistenceEnabled();
    }

    private SecurityService createSecurityService(Node node) {
        return node.getConfig().getSecurityConfig().isEnabled() ? new SecurityServiceImpl(node) : null;
    }

    private HotBackupService createHotBackupService(
            Node node, HotRestartIntegrationService hotRestartService) {
        if (hotRestartService == null) {
            return null;
        }
        final HotRestartPersistenceConfig config = node.getConfig().getHotRestartPersistenceConfig();
        return config.getBackupDir() != null ? new HotBackupService(node, hotRestartService) : null;
    }

    private AuditlogService createAuditlogService(Node node) {
        if (node.getProperties().getBoolean(ClusterProperty.AUDIT_LOG_ENABLED)) {
            return new ILoggerAuditlogService(node.getLoggingService());
        }
        return NoOpAuditlogService.INSTANCE;
    }

    private HotRestartIntegrationService createHotRestartService(Node node) {
        HotRestartPersistenceConfig hotRestartPersistenceConfig = node.getConfig().getHotRestartPersistenceConfig();
        return hotRestartPersistenceConfig.isEnabled() ? new HotRestartIntegrationService(node) : null;
    }

    @Override
    public void beforeStart() {
        // NLC mode check for a "built-in license"
        license = LicenseHelper.getBuiltInLicense();
        if (license == null) {
            logger.log(Level.INFO, "Checking Hazelcast Enterprise license...");

            String licenseKey = node.getProperties().getString(ClusterProperty.ENTERPRISE_LICENSE_KEY);
            if (licenseKey == null || licenseKey.isEmpty()) {
                licenseKey = node.getConfig().getLicenseKey();
            }
            setLicenseInternal(LicenseHelper.getLicense(licenseKey, buildInfo.getVersion()), licenseKey);
        } else {
            logger.log(Level.FINE, "This is an OEM version of Hazelcast Enterprise.");
        }

        createSecurityContext(node);
        createMemoryManager(node);
    }

    private void setLicenseInternal(License lic, String licenseKey) {
        license = lic;
        node.config.setLicenseKey(licenseKey);
        logger.log(Level.INFO, license.toString());
    }

    @Override
    public void setLicenseKey(String licenseKey) {
        License userLicense = LicenseHelper.getLicense(licenseKey, buildInfo.getVersion());
        checkLicenseCompatible(license, userLicense);

        synchronized (this) {
            setLicenseInternal(userLicense, licenseKey);
            onLicenseChanged(userLicense);
        }

        /* No need to recreate the security context and the memory manager
           since the licensed features don't change. */

        logger.log(Level.WARNING, "License updated at run time - please make sure to update the license "
                + "in the persistent configuration to avoid losing the changes on restart.");
    }

    private void onLicenseChanged(License newLicense) {
        // license reminder task
        initLicenseExpReminder(newLicense);

        // license info MBean
        initLicenseMBean(newLicense);
    }

    private static void checkLicenseCompatible(License current, License newLicense) {
        if (newLicense.getExpiryDate().getTime() < current.getExpiryDate().getTime()) {
            throw new InvalidLicenseException("License expires before the current license");
        }

        if (current.getVersion().getCode() > newLicense.getVersion().getCode()) {
            throw new InvalidLicenseException("Cannot update to an older version license");
        }

        if (newLicense.getAllowedNumberOfNodes() < current.getAllowedNumberOfNodes()) {
            throw new InvalidLicenseException("License allows a smaller number of nodes "
                    + newLicense.getAllowedNumberOfNodes() + " than the current license " + current.getAllowedNumberOfNodes());
        }
        checkFeaturesCompatible(current, newLicense);
    }

    private static void checkFeaturesCompatible(License current, License newLicense) {
        LicenseVersion fromVersion = current.getVersion();
        LicenseVersion toVersion = newLicense.getVersion();

        Set<Feature> removals = collectFeaturesFromFeatureMap(fromVersion, toVersion, LICENSE_FEATURE_REMOVALS);
        Set<Feature> additions = collectFeaturesFromFeatureMap(fromVersion, toVersion, LICENSE_FEATURE_ADDITIONS);

        Set<Feature> currentFeatures
                = current.getFeatures() == null ? EnumSet.noneOf(Feature.class) : EnumSet.copyOf(current.getFeatures());
        // remove legacy features no longer present in versions > current
        // (nothing to compare against in the new license version)
        currentFeatures.removeAll(removals);

        Set<Feature> newFeatures = newLicense.getFeatures() == null ? EnumSet.noneOf(Feature.class)
                : EnumSet.copyOf(newLicense.getFeatures());
        // remove features introduced in versions > current
        // (nothing to compare against in the current license version)
        newFeatures.removeAll(additions);

        if (!newFeatures.equals(currentFeatures)) {
            throw new InvalidLicenseException("License has incompatible features "
                    + newLicense.getFeatures() + " with the current license " + current.getFeatures());
        }
    }

    private static Set<Feature> collectFeaturesFromFeatureMap(LicenseVersion fromVersion, LicenseVersion toVersion,
                                                              Map<LicenseVersion, Set<Feature>> featureMap) {
        if (fromVersion.getCode() >= toVersion.getCode()) {
            return Collections.emptySet();
        }
        Set<Feature> features = EnumSet.noneOf(Feature.class);
        for (LicenseVersion version : LicenseVersion.values()) {
            if (version.getCode() > fromVersion.getCode() && version.getCode() <= toVersion.getCode()) {
                Set<Feature> featureSet = featureMap.get(version);
                if (featureSet != null) {
                    features.addAll(featureSet);
                }
            }
        }
        return features;
    }

    private boolean isRollingUpgradeLicensed() {
        return license.getFeatures().contains(Feature.ROLLING_UPGRADE);
    }

    private void createSecurityContext(Node node) {
        boolean securityEnabled = node.getConfig().getSecurityConfig().isEnabled();
        if (securityEnabled) {
            LicenseHelper.checkLicensePerFeature(license, Feature.SECURITY);
            securityContext = new SecurityContextImpl(node);
        }
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
    @SuppressFBWarnings("RV_RETURN_VALUE_OF_PUTIFABSENT_IGNORED")
    private MemberSocketInterceptor createOrGetSocketInterceptor(EndpointQualifier qualifier) {
        qualifier = qualifier == null ? MEMBER : qualifier;
        MemberSocketInterceptor si = socketInterceptors.get(qualifier);
        if (si != null) {
            return si;
        }

        SocketInterceptor implementation = null;
        SocketInterceptorConfig socketInterceptorConfig = getSocketInterceptorConfig(qualifier);

        if (socketInterceptorConfig != null && socketInterceptorConfig.isEnabled()) {
            implementation = (SocketInterceptor) socketInterceptorConfig.getImplementation();
            if (implementation == null && socketInterceptorConfig.getClassName() != null) {
                try {
                    implementation = (SocketInterceptor) Class.forName(socketInterceptorConfig.getClassName()).newInstance();
                } catch (Throwable e) {
                    logger.severe("SocketInterceptor class cannot be instantiated!" + socketInterceptorConfig.getClassName(), e);
                }
            }
            if (implementation != null) {
                if (!(implementation instanceof MemberSocketInterceptor)) {
                    logger.severe("SocketInterceptor must be instance of " + MemberSocketInterceptor.class.getName());
                    implementation = null;
                }
            }
        }

        si = (MemberSocketInterceptor) implementation;
        if (si != null) {
            logger.info("SocketInterceptor is enabled");
            si.init(socketInterceptorConfig.getProperties());
            socketInterceptors.putIfAbsent(qualifier, si);
        }

        return si;
    }

    @Override
    public void beforeJoin() {
        if (hotRestartService != null) {
            LicenseHelper.checkLicensePerFeature(license, Feature.HOT_RESTART);
            hotRestartService.prepare();
        }

        // Creating CpPersistenceService after node is fully initialized.
        cpPersistenceService = createCPPersistenceService(node);
        if (cpPersistenceService != null) {
            LicenseHelper.checkLicensePerFeature(license, Feature.CP_PERSISTENCE);
        }
        if (node.getConfig().getNativeMemoryConfig().isEnabled()) {
            LicenseHelper.checkLicensePerFeature(license, Feature.HD_MEMORY);
        }
    }

    private CPPersistenceServiceImpl createCPPersistenceService(Node node) {
        return cpPersistenceEnabled ? new CPPersistenceServiceImpl(node) : null;
    }

    @Override
    public void printNodeInfo() {
        String build = buildInfo.getBuild();
        String revision = buildInfo.getRevision();
        if (!revision.isEmpty()) {
            build += " - " + revision;
            BuildInfo upstreamBuildInfo = buildInfo.getUpstreamBuildInfo();
            if (upstreamBuildInfo != null) {
                String upstreamRevision = upstreamBuildInfo.getRevision();
                if (!isNullOrEmpty(upstreamRevision)) {
                    build += ", " + upstreamRevision;
                }
            }
        }

        WeakSecretsConfigChecker configChecker = new WeakSecretsConfigChecker(node.getConfig());
        configChecker.evaluateAndReport(systemLogger);

        systemLogger.info("Hazelcast Enterprise " + buildInfo.getVersion()
                + " (" + build + ") starting at " + node.getThisAddress());
        systemLogger.info("Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.");
    }

    @Override
    @SuppressWarnings("checkstyle:npathcomplexity")
    public void afterStart() {
        if (license == null) {
            logger.log(Level.SEVERE, "Hazelcast Enterprise license could not be found!");
            node.shutdown(true);
            return;
        }

        final int nodeCount = node.getClusterService().getSize();
        if (nodeCount > license.getAllowedNumberOfNodes()) {
            logger.log(Level.SEVERE, "Exceeded maximum number of nodes allowed in Hazelcast Enterprise license! Max: "
                    + license.getAllowedNumberOfNodes() + ", Current: " + nodeCount);
            node.shutdown(true);
            return;
        }

        if (cpPersistenceService != null) {
            try {
                cpPersistenceService.start();
            } catch (Throwable e) {
                logger.severe("CP restore failed", e);
                node.shutdown(true);
                return;
            }
        }

        if (hotRestartService != null) {
            try {
                hotRestartService.start();
            } catch (Throwable e) {
                logger.severe("Hot Restart procedure failed", e);
                node.shutdown(true);
                return;
            }
        }

        refreshClusterPermissions();

        if (memoryManager != null) {
            // (<native_memory_size> * <node_count>) / (2 * <partition_count>)
            // `2` comes from default backup count is `1` so by default there are primary and backup partitions.
            final MemoryStats memoryStats = memoryManager.getMemoryStats();
            final int maxNativeMemorySizeInMegaBytes = (int) MemoryUnit.BYTES.toMegaBytes(memoryStats.getMaxNative());
            final int partitionCount = node.getPartitionService().getPartitionCount();
            final int nativeMemorySizePerPartition = (maxNativeMemorySizeInMegaBytes * nodeCount) / (2 * partitionCount);
            if (nativeMemorySizePerPartition > SUGGESTED_MAX_NATIVE_MEMORY_SIZE_PER_PARTITION_IN_MB) {
                logger.warning(String.format("Native memory size per partition (%d MB) is higher than "
                                + "suggested maximum native memory size per partition (%d MB). "
                                + "You may think increasing partition count which is `%d` at the moment.",
                        nativeMemorySizePerPartition,
                        SUGGESTED_MAX_NATIVE_MEMORY_SIZE_PER_PARTITION_IN_MB,
                        partitionCount));
            }
        }

        initWanConsumers();
        initLicenseExpReminder(license);
    }

    private void refreshClusterPermissions() {
        SecurityConfig securityConfig = node.getConfig().getSecurityConfig();
        if (securityService != null && securityConfig.getOnJoinPermissionOperation() == OnJoinPermissionOperationName.SEND) {
            logger.info("Refreshing client permissions in cluster");
            securityService.refreshClientPermissions(securityConfig.getClientPermissionConfigs());
        }
    }

    private void initLicenseExpReminder(License lic) {
        boolean builtInLicense = LicenseHelper.isBuiltInLicense(lic);
        try {
            if (licenseExpirationReminderTask == null) {
                // schedule a new task
                if (!builtInLicense) {
                    TaskScheduler taskScheduler = node.nodeEngine.getExecutionService().getGlobalTaskScheduler();
                    licenseExpirationReminderTask = LicenseExpirationReminderTask.scheduleWith(taskScheduler, lic);
                }
            } else {
                // cancel/reschedule the current task
                licenseExpirationReminderTask
                        = licenseExpirationReminderTask.rescheduleWithNewLicense(builtInLicense ? null : lic);
            }
        } catch (RejectedExecutionException e) {
            if (node.isRunning()) {
                throw e;
            }
        }
    }

    private void initLicenseMBean(License lic) {
        // if a license MBean exists, unregisters it and register a new one
        ManagementService managementService = node.hazelcastInstance.getManagementService();
        EnterpriseManagementService.EnterpriseInstanceMBean instanceMBean
                = (EnterpriseManagementService.EnterpriseInstanceMBean) managementService.getInstanceMBean();
        if (instanceMBean != null) {
            LicenseInfoMBean licenseInfoMBean = instanceMBean.getLicenseInfoMBean();
            HazelcastMBean.unregister(licenseInfoMBean);
            HazelcastMBean.register(new LicenseInfoMBean(new LicenseInfoImpl(lic), node, managementService));
        }
    }

    /**
     * Constructs and initializes all WAN consumers defined in the WAN
     * configuration
     *
     * @see com.hazelcast.wan.WanReplicationConsumer
     */
    private void initWanConsumers() {
        WanReplicationService wanReplicationService = node.nodeEngine.getWanReplicationService();
        if (wanReplicationService instanceof EnterpriseWanReplicationService) {
            ((EnterpriseWanReplicationService) wanReplicationService).initializeCustomConsumers();
        }
    }

    @Override
    public boolean isStartCompleted() {
        boolean hotRestartStartCompleted = true;
        if (hotRestartService != null) {
            hotRestartStartCompleted = hotRestartService.isStartCompleted();
        }

        boolean cpPersistenceStartCompleted = true;
        if (cpPersistenceService != null) {
            cpPersistenceStartCompleted = cpPersistenceService.isStartCompleted();
        }

        return hotRestartStartCompleted && cpPersistenceStartCompleted && super.isStartCompleted();
    }

    public License getLicense() {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(
                    new HazelcastRuntimePermission("com.hazelcast.instance.impl.EnterpriseNodeExtension.getLicense"));
        }
        return license;
    }

    @Override
    public InternalSerializationService createSerializationService() {
        InternalSerializationService ss;
        try {
            Config config = node.getConfig();
            ClassLoader configClassLoader = node.getConfigClassLoader();

            HazelcastInstanceImpl hazelcastInstance = node.hazelcastInstance;
            PartitioningStrategy partitioningStrategy = getPartitioningStrategy(configClassLoader);

            EnterpriseClusterVersionListener listener = new EnterpriseClusterVersionListener();
            registerListener(listener);

            EnterpriseSerializationServiceBuilder builder = new EnterpriseSerializationServiceBuilder();
            SerializationConfig serializationConfig = config.getSerializationConfig() != null ? config
                    .getSerializationConfig() : new SerializationConfig();

            ss = builder
                    .setMemoryManager(memoryManager)
                    .setClassLoader(configClassLoader)
                    .setConfig(serializationConfig)
                    .setManagedContext(hazelcastInstance.managedContext)
                    .setPartitioningStrategy(partitioningStrategy)
                    .setClusterVersionAware(listener)
                    .setHazelcastInstance(hazelcastInstance)
                    // EE version uses versioned serialization when rolling upgrade feature is licensed
                    .setVersionedSerializationEnabled(isRollingUpgradeLicensed())
                    .setNotActiveExceptionSupplier(new Supplier<RuntimeException>() {
                        @Override
                        public RuntimeException get() {
                            return new HazelcastInstanceNotActiveException();
                        }
                    })
                    .build();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        return ss;
    }

    @Override
    public void scheduleClusterVersionAutoUpgrade() {
        if (isRollingUpgradeLicensed()) {
            clusterVersionAutoUpgradeHelper.scheduleNewAutoUpgradeTask(node.clusterService);
        }
    }

    private void createMemoryManager(Node node) {
        final NativeMemoryConfig memoryConfig = node.getConfig().getNativeMemoryConfig();

        if (memoryConfig.isEnabled()) {
            MemorySize size = memoryConfig.getSize();
            NativeMemoryConfig.MemoryAllocatorType type = memoryConfig.getAllocatorType();
            final FreeMemoryChecker freeMemoryChecker = new FreeMemoryChecker(node.getProperties());

            logger.info("Creating " + type + " native memory manager with " + size.toPrettyString() + " size");
            LibMallocFactory libMallocFactory = createLibMallocFactory(node, memoryConfig, freeMemoryChecker);
            if (type == NativeMemoryConfig.MemoryAllocatorType.STANDARD) {
                if (isHotRestartEnabled()) {
                    throw new InvalidConfigurationException("MemoryAllocatorType.STANDARD cannot be used when Hot Restart "
                            + "is enabled. Please use MemoryAllocatorType.POOLED!");
                }
                memoryManager = new StandardMemoryManager(size, libMallocFactory);
            } else {
                int blockSize = memoryConfig.getMinBlockSize();
                int pageSize = memoryConfig.getPageSize();
                float metadataSpace = memoryConfig.getMetadataSpacePercentage();
                memoryManager = isHotRestartEnabled()
                        ? new HotRestartPoolingMemoryManager(size, blockSize, pageSize, metadataSpace,
                        libMallocFactory)
                        : new PoolingMemoryManager(size, blockSize, pageSize, metadataSpace, libMallocFactory);
            }
        }
    }

    private static LibMallocFactory createLibMallocFactory(Node node, NativeMemoryConfig config,
                                                           FreeMemoryChecker freeMemoryChecker) {
        if (config.getPersistentMemoryDirectory() == null) {
            // RAM via Unsafe
            return new UnsafeMallocFactory(freeMemoryChecker);
        } else {
            // Non-volatile memory
            return new PersistentMemoryMallocFactory(config);
        }
    }

    @Override
    public SecurityContext getSecurityContext() {
        return securityContext;
    }

    @Override
    public SecurityService getSecurityService() {
        return securityService;
    }

    @Override
    public MemberSocketInterceptor getSocketInterceptor(EndpointQualifier endpointQualifier) {
        return createOrGetSocketInterceptor(endpointQualifier);
    }

    @Override
    public InboundHandler[] createInboundHandlers(EndpointQualifier qualifier, TcpIpConnection connection, IOService ioService) {
        SymmetricEncryptionConfig symmetricEncryptionConfig = ioService.getSymmetricEncryptionConfig(qualifier);

        if (symmetricEncryptionConfig != null && symmetricEncryptionConfig.isEnabled()) {
            logger.info(qualifier + " reader started with SymmetricEncryption");
            SymmetricCipherPacketDecoder decoder = new SymmetricCipherPacketDecoder(symmetricEncryptionConfig,
                    connection, ioService, node.nodeEngine.getPacketDispatcher());
            return new InboundHandler[]{decoder};
        }

        return super.createInboundHandlers(qualifier, connection, ioService);
    }

    @Override
    public OutboundHandler[] createOutboundHandlers(EndpointQualifier qualifier,
                                                    TcpIpConnection connection, IOService ioService) {
        SymmetricEncryptionConfig symmetricEncryptionConfig = ioService.getSymmetricEncryptionConfig(qualifier);

        if (symmetricEncryptionConfig != null && symmetricEncryptionConfig.isEnabled()) {
            logger.info(qualifier + " writer started with SymmetricEncryption");
            SymmetricCipherPacketEncoder encoder = new SymmetricCipherPacketEncoder(connection, symmetricEncryptionConfig);
            return new OutboundHandler[]{encoder};
        }

        return super.createOutboundHandlers(qualifier, connection, ioService);
    }

    @Override
    public ChannelInitializerProvider createChannelInitializerProvider(IOService ioService) {
        EnterpriseChannelInitializerProvider provider = new EnterpriseChannelInitializerProvider(ioService, node);
        provider.init();
        return provider;
    }

    @Override
    public void onThreadStart(Thread thread) {
        registerThreadToPoolingMemoryManager(thread);
    }

    private void registerThreadToPoolingMemoryManager(Thread thread) {
        if (!(thread instanceof PartitionOperationThread)) {
            return;
        }
        EnterpriseSerializationService serializationService
                = (EnterpriseSerializationService) node.getSerializationService();

        HazelcastMemoryManager memoryManager = serializationService.getMemoryManager();
        if (memoryManager instanceof PoolingMemoryManager) {
            ((PoolingMemoryManager) memoryManager).registerThread(thread);
        }
    }

    @Override
    public void onThreadStop(Thread thread) {
        deregisterThreadFromPoolingMemoryManager(thread);
    }

    private void deregisterThreadFromPoolingMemoryManager(Thread thread) {
        EnterpriseSerializationService serializationService
                = (EnterpriseSerializationService) node.getSerializationService();

        HazelcastMemoryManager memoryManager = serializationService.getMemoryManager();
        if (memoryManager instanceof PoolingMemoryManager) {
            ((PoolingMemoryManager) memoryManager).deregisterThread(thread);
        }
    }

    @Override
    public InternalHotRestartService getInternalHotRestartService() {
        if (hotRestartService == null) {
            return NOOP_INTERNAL_HOT_RESTART_SERVICE;
        }
        return hotRestartService;
    }

    public boolean isHotRestartEnabled() {
        return hotRestartService != null;
    }

    public boolean isFeatureEnabledForLicenseKey(Feature feature) {
        boolean enabled = true;
        try {
            LicenseHelper.checkLicensePerFeature(license, feature);
        } catch (InvalidLicenseException e) {
            enabled = false;
            logger.warning(e.getMessage());
        }
        return enabled;
    }

    @Override
    public void beforeShutdown() {
        super.beforeShutdown();
        if (hotRestartService != null) {
            hotRestartService.shutdown();
        }
        if (cpPersistenceService != null) {
            cpPersistenceService.shutdown();
        }
    }

    @Override
    public void shutdown() {
        super.shutdown();
        license = null;
    }

    @Override
    public <T> T createService(Class<T> clazz) {
        if (WanReplicationService.class.isAssignableFrom(clazz)) {
            try {
                LicenseHelper.checkLicensePerFeature(license, Feature.WAN);
                return (T) new EnterpriseWanReplicationService(node);
            } catch (InvalidLicenseException e) {
                return (T) new WanReplicationServiceImpl(node);
            }
        } else if (ICacheService.class.isAssignableFrom(clazz)) {
            return (T) new EnterpriseCacheService();
        } else if (MapService.class.isAssignableFrom(clazz)) {
            return (T) getEnterpriseMapServiceConstructor().createNew(node.getNodeEngine());
        }
        throw new IllegalArgumentException("Unknown service class: " + clazz);
    }

    @Override
    public Map<String, Object> createExtensionServices() {
        Map<String, Object> services = new HashMap<String, Object>(super.createExtensionServices());

        if (hotRestartService != null) {
            services.put(HotRestartIntegrationService.SERVICE_NAME, hotRestartService);
        }
        if (hotBackupService != null) {
            services.put(HotBackupService.SERVICE_NAME, hotBackupService);
        }
        if (securityService != null) {
            services.put(SecurityServiceImpl.SERVICE_NAME, securityService);
        }

        return services;
    }

    @Override
    public MemoryStats getMemoryStats() {
        HazelcastMemoryManager mm = memoryManager;
        return mm != null ? mm.getMemoryStats() : super.getMemoryStats();
    }

    @Override
    public void validateJoinRequest(JoinMessage joinRequest) {
        if (isRollingUpgradeLicensed()) {
            validateJoiningMemberVersion(joinRequest);
        } else {
            // when license does not allow RU, apply OS rules for validating join request
            super.validateJoinRequest(joinRequest);
        }
        NativeMemoryConfig memoryConfig = node.getConfig().getNativeMemoryConfig();
        if (!memoryConfig.isEnabled()) {
            return;
        }
        // 3.5 license keys have limited HD so we check available HD memory
        if (license.getVersion() == LicenseVersion.V2) {
            long totalNativeMemorySize = node.getClusterService().getSize() * memoryConfig.getSize().bytes();
            long licensedNativeMemorySize = MemoryUnit.GIGABYTES.toBytes(license.getAllowedNativeMemorySize());
            if (totalNativeMemorySize >= licensedNativeMemorySize) {
                throw new InvalidLicenseException("Total native memory of cluster exceeds licensed native memory."
                        + " Please contact sales@hazelcast.com");
            }
        }
    }

    // validate that the joining member is at same major and >= minor version as the cluster version at which this cluster
    // operates
    private void validateJoiningMemberVersion(JoinMessage joinMessage) {
        Version clusterVersion = node.getClusterService().getClusterVersion();
        MemberVersion memberVersion = joinMessage.getMemberVersion();

        String msg = "Joining node's version " + memberVersion + " is not compatible with cluster version " + clusterVersion;
        if (clusterVersion.getMajor() != memberVersion.getMajor()) {
            msg += " (Rolling Member Upgrades are only supported for the same major version)";
            throw new VersionMismatchException(msg);
        }
        if (clusterVersion.getMinor() > memberVersion.getMinor()) {
            msg += " (Rolling Member Upgrades are only supported for the next minor version)";
            throw new VersionMismatchException(msg);
        }
    }

    /**
     * Check if this node's version is compatible with given cluster version.
     * By default, For rolling upgrades context, a node's version is considered compatible with cluster of same version
     * or if cluster's minor version number is smaller by one versus node's minor version number.
     * Each version may overwrite it though by modifying this method.
     * <p>
     * E.g.:
     * 3.8 does not support any emulation at all. So it's compatible with 3.8 clusters only.
     * 3.9 will be compatible with 3.8 and 3.9 cluster -> if not overwritten in 3.9 codebase.
     *
     * @param clusterVersion the cluster version to check against
     * @return {@code true} if compatible, otherwise false.
     */
    @Override
    public boolean isNodeVersionCompatibleWith(Version clusterVersion) {
        if (!isRollingUpgradeLicensed()) {
            // when license does not allow RU, apply OS rules: node version is only
            // compatible with its own major.minor cluster version
            return super.isNodeVersionCompatibleWith(clusterVersion);
        }

        Preconditions.checkNotNull(clusterVersion);

        // there is no compatibility between major versions
        Version nodeVersion = node.getVersion().asVersion();
        if (node.getVersion().getMajor() != clusterVersion.getMajor()) {
            return false;
        }

        // We don't have to check prior to "3.8" since 3.8 adds rolling-upgrade support
        // and the check is invoked by the node for this particular node (so for the current codebase version).
        // It means that this method didn't exist prior to 3.8.

        if (nodeVersion.equals(Version.of("3.8"))) {
            // 3.8 can't emulate lower versions
            return node.getVersion().getMinor() == clusterVersion.getMinor();
        } else {
            // node can either work at its codebase version (native mode)
            // or at a minor version that's smaller by one (emulated mode)
            // it may be overwritten by each version though
            // so 3.10 may support emulating 3.8 or 3.9
            return node.getVersion().getMinor() == clusterVersion.getMinor()
                    || (node.getVersion().getMinor() - clusterVersion.getMinor() == 1);
        }
    }

    @Override
    public void onInitialClusterState(ClusterState initialState) {
        super.onInitialClusterState(initialState);

        if (hotRestartService != null) {
            hotRestartService.onInitialClusterState(initialState);
        }
    }

    @Override
    public void onClusterStateChange(ClusterState newState, boolean isTransient) {
        super.onClusterStateChange(newState, isTransient);

        if (hotRestartService != null && !isTransient) {
            hotRestartService.getClusterMetadataManager().onClusterStateChange(newState);
        }
    }

    @Override
    public void onPartitionStateChange() {
        super.onPartitionStateChange();

        if (hotRestartService != null) {
            hotRestartService.getClusterMetadataManager().onPartitionStateChange();
        }
    }

    @Override
    public void onMemberListChange() {
        super.onMemberListChange();

        if (hotRestartService != null) {
            hotRestartService.getClusterMetadataManager().onMembershipChange();
        }
    }

    @Override
    public void onClusterVersionChange(Version newVersion) {
        super.onClusterVersionChange(newVersion);
        if (hotRestartService != null) {
            hotRestartService.getClusterMetadataManager().onClusterVersionChange(newVersion);
        }
    }

    @Override
    public boolean registerListener(Object listener) {
        super.registerListener(listener);
        if (listener instanceof ClusterHotRestartEventListener) {
            if (hotRestartService == null) {
                throw new HotRestartException("HotRestart is not enabled!");
            }
            hotRestartService.addClusterHotRestartEventListener((ClusterHotRestartEventListener) listener);
            return true;
        }
        return false;
    }

    public HazelcastMemoryManager getMemoryManager() {
        return memoryManager;
    }

    @Override
    public HotRestartService getHotRestartService() {
        if (hotBackupService == null) {
            return NO_OP_HOT_RESTART_SERVICE;
        }
        return hotBackupService;
    }

    @Override
    public UUID createMemberUuid() {
        if (hotRestartService != null) {
            UUID uuid = hotRestartService.getClusterMetadataManager().readMemberUuid();
            if (uuid != null) {
                return uuid;
            }
        }
        return super.createMemberUuid();
    }

    @Override
    public void provideStaticMetrics(MetricsRegistry registry) {
        if (memoryManager != null) {
            registry.registerStaticMetrics(memoryManager, "memorymanager");
            if (memoryManager instanceof StaticMetricsProvider) {
                ((StaticMetricsProvider) memoryManager).provideStaticMetrics(registry);
            }
        }
    }

    @Override
    public TimedMemberStateFactory createTimedMemberStateFactory(HazelcastInstanceImpl instance) {
        return new EnterpriseTimedMemberStateFactory(instance);
    }

    @Override
    public ManagementCenterConnectionFactory getManagementCenterConnectionFactory() {
        return new EnterpriseManagementCenterConnectionFactory();
    }

    @Override
    public ByteArrayProcessor createMulticastInputProcessor(IOService ioService) {
        final SymmetricEncryptionConfig symmetricEncryptionConfig = ioService.getSymmetricEncryptionConfig(MEMBER);

        if (symmetricEncryptionConfig != null && symmetricEncryptionConfig.isEnabled()) {
            logger.info("Multicast is starting with SymmetricEncryption on input processor");
            return new CipherByteArrayProcessor(createSymmetricReaderCipher(symmetricEncryptionConfig));
        }

        return super.createMulticastInputProcessor(ioService);
    }

    @Override
    public ByteArrayProcessor createMulticastOutputProcessor(IOService ioService) {
        final SymmetricEncryptionConfig symmetricEncryptionConfig = ioService.getSymmetricEncryptionConfig(MEMBER);

        if (symmetricEncryptionConfig != null && symmetricEncryptionConfig.isEnabled()) {
            logger.info("Multicast is starting with SymmetricEncryption on output processor");
            return new CipherByteArrayProcessor(createSymmetricWriterCipher(symmetricEncryptionConfig));
        }

        return super.createMulticastOutputProcessor(ioService);
    }

    @Override
    public void registerPlugins(Diagnostics diagnostics) {
        super.registerPlugins(diagnostics);
        diagnostics.register(new WANPlugin(node.nodeEngine));
    }

    @Override
    public DynamicConfigListener createDynamicConfigListener() {
        if (hotRestartService == null) {
            return super.createDynamicConfigListener();
        }
        return new HotRestartConfigListener(hotRestartService);
    }

    @Override
    public ManagementService createJMXManagementService(HazelcastInstanceImpl instance) {
        return new EnterpriseManagementService(instance);
    }

    @Override
    public TextCommandService createTextCommandService() {
        return new EnterpriseTextCommandServiceImpl(node);
    }

    @Override
    protected void createAndSetPhoneHome() {
        super.phoneHome = new EnterprisePhoneHome(node);
    }

    private SocketInterceptorConfig getSocketInterceptorConfig(EndpointQualifier qualifier) {
        AdvancedNetworkConfig advancedNetworkConfig = node.getConfig().getAdvancedNetworkConfig();
        SocketInterceptorConfig socketInterceptorConfig;
        if (advancedNetworkConfig.isEnabled()) {
            socketInterceptorConfig = advancedNetworkConfig.getEndpointConfigs().get(qualifier).getSocketInterceptorConfig();
        } else {
            socketInterceptorConfig = node.getConfig().getNetworkConfig().getSocketInterceptorConfig();
        }
        return socketInterceptorConfig;
    }

    @Override
    public boolean isClientFailoverSupported() {
        return true;
    }

    @Override
    public AuditlogService getAuditlogService() {
        return auditlogService;
    }

    @Override
    public CPPersistenceService getCPPersistenceService() {
        if (cpPersistenceEnabled) {
            if (cpPersistenceService == null) {
                throw new IllegalStateException("CP persistence service is not initialized yet!");
            }
            return cpPersistenceService;
        }
        return super.getCPPersistenceService();
    }
}
