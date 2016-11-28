package com.hazelcast.instance;

import com.hazelcast.cache.EnterpriseCacheService;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigurationException;
import com.hazelcast.config.HotRestartPersistenceConfig;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.internal.cluster.impl.JoinMessage;
import com.hazelcast.internal.cluster.impl.VersionMismatchException;
import com.hazelcast.internal.management.dto.ClusterHotRestartStatusDTO;
import com.hazelcast.internal.networking.ReadHandler;
import com.hazelcast.internal.networking.SocketChannelWrapperFactory;
import com.hazelcast.internal.networking.WriteHandler;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.EnterpriseClusterVersionListener;
import com.hazelcast.internal.serialization.impl.EnterpriseSerializationServiceBuilder;
import com.hazelcast.license.domain.Feature;
import com.hazelcast.license.domain.License;
import com.hazelcast.license.domain.LicenseVersion;
import com.hazelcast.license.exception.InvalidLicenseException;
import com.hazelcast.license.util.LicenseHelper;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.memory.FreeMemoryChecker;
import com.hazelcast.memory.HazelcastMemoryManager;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryStats;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.memory.PoolingMemoryManager;
import com.hazelcast.memory.StandardMemoryManager;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.MemberSocketInterceptor;
import com.hazelcast.nio.SocketInterceptor;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.nio.ssl.SSLSocketChannelWrapperFactory;
import com.hazelcast.nio.tcp.SymmetricCipherMemberReadHandler;
import com.hazelcast.nio.tcp.SymmetricCipherMemberWriteHandler;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.security.SecurityContextImpl;
import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.hotrestart.HotRestartService;
import com.hazelcast.spi.hotrestart.cluster.ClusterHotRestartEventListener;
import com.hazelcast.spi.hotrestart.cluster.ClusterHotRestartStatusDTOUtil;
import com.hazelcast.spi.hotrestart.cluster.ClusterMetadataManager;
import com.hazelcast.spi.hotrestart.memory.HotRestartPoolingMemoryManager;
import com.hazelcast.spi.impl.operationexecutor.impl.PartitionOperationThread;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.Preconditions;
import com.hazelcast.version.ClusterVersion;
import com.hazelcast.wan.WanReplicationService;
import com.hazelcast.wan.impl.WanReplicationServiceImpl;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

import static com.hazelcast.map.impl.EnterpriseMapServiceConstructor.getEnterpriseMapServiceConstructor;
import static com.hazelcast.spi.hotrestart.cluster.HotRestartClusterStartStatus.CLUSTER_START_SUCCEEDED;

/**
 * This class is the enterprise system hook to allow injection of enterprise services into Hazelcast subsystems.
 */
@SuppressWarnings({
        "checkstyle:classdataabstractioncoupling",
        "checkstyle:classfanoutcomplexity",
        "checkstyle:methodcount"
})
public class EnterpriseNodeExtension extends DefaultNodeExtension implements NodeExtension {

    private static final int SUGGESTED_MAX_NATIVE_MEMORY_SIZE_PER_PARTITION_IN_MB = 256;

    private final HotRestartService hotRestartService;
    private volatile License license;
    private volatile SecurityContext securityContext;
    private volatile MemberSocketInterceptor memberSocketInterceptor;
    private volatile HazelcastMemoryManager memoryManager;

    private final BuildInfo buildInfo = BuildInfoProvider.getBuildInfo();

    public EnterpriseNodeExtension(Node node) {
        super(node);
        hotRestartService = createHotRestartService(node);
    }

    private HotRestartService createHotRestartService(Node node) {
        HotRestartPersistenceConfig hotRestartPersistenceConfig = node.getConfig().getHotRestartPersistenceConfig();
        return hotRestartPersistenceConfig.isEnabled() ? new HotRestartService(node) : null;
    }

    @Override
    public void beforeStart() {
        logger.log(Level.INFO, "Checking Hazelcast Enterprise license...");

        String licenseKey = node.getProperties().getString(GroupProperty.ENTERPRISE_LICENSE_KEY);
        if (licenseKey == null || licenseKey.isEmpty()) {
            licenseKey = node.getConfig().getLicenseKey();
        }
        license = LicenseHelper.getLicense(licenseKey, buildInfo.getVersion());
        logger.log(Level.INFO, license.toString());

        createSecurityContext(node);
        createMemoryManager(node);
        createSocketInterceptor(node.config.getNetworkConfig());
    }

    private void createSecurityContext(Node node) {
        boolean securityEnabled = node.getConfig().getSecurityConfig().isEnabled();
        if (securityEnabled) {
            LicenseHelper.checkLicenseKeyPerFeature(license.getKey(), buildInfo.getVersion(),
                    Feature.SECURITY);
            securityContext = new SecurityContextImpl(node);
        }
    }

    private void createSocketInterceptor(NetworkConfig networkConfig) {
        SocketInterceptorConfig sic = networkConfig.getSocketInterceptorConfig();
        SocketInterceptor implementation = null;
        if (sic != null && sic.isEnabled()) {
            implementation = (SocketInterceptor) sic.getImplementation();
            if (implementation == null && sic.getClassName() != null) {
                try {
                    implementation = (SocketInterceptor) Class.forName(sic.getClassName()).newInstance();
                } catch (Throwable e) {
                    logger.severe("SocketInterceptor class cannot be instantiated!" + sic.getClassName(), e);
                }
            }
            if (implementation != null) {
                if (!(implementation instanceof MemberSocketInterceptor)) {
                    logger.severe("SocketInterceptor must be instance of " + MemberSocketInterceptor.class.getName());
                    implementation = null;
                }
            }
        }

        memberSocketInterceptor = (MemberSocketInterceptor) implementation;
        if (memberSocketInterceptor != null) {
            logger.info("SocketInterceptor is enabled");
            memberSocketInterceptor.init(sic.getProperties());
        }
    }

    @Override
    public void beforeJoin() {
        if (hotRestartService != null) {
            LicenseHelper.checkLicenseKeyPerFeature(license.getKey(), buildInfo.getVersion(),
                    Feature.HOT_RESTART);
            hotRestartService.prepare();
        }
    }

    @Override
    public void printNodeInfo() {
        String build = buildInfo.getBuild();
        String revision = buildInfo.getRevision();
        if (!revision.isEmpty()) {
            build += " - " + revision;
        }

        systemLogger.info("Hazelcast Enterprise " + buildInfo.getVersion()
                + " (" + build + ") starting at " + node.getThisAddress());
        systemLogger.info("Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.");
    }

    @Override
    public void afterStart() {
        if (license == null) {
            logger.log(Level.SEVERE, "Hazelcast Enterprise license could not be found!");
            node.shutdown(true);
            return;
        }
        final int nodeCount = node.getClusterService().getSize();
        if (nodeCount > license.getAllowedNumberOfNodes()) {
            logger.log(Level.SEVERE,
                    "Exceeded maximum number of nodes allowed in Hazelcast Enterprise license! Max: "
                            + license.getAllowedNumberOfNodes() + ", Current: " + nodeCount);
            node.shutdown(true);
        }

        if (hotRestartService != null) {
            try {
                hotRestartService.start();
            } catch (Throwable e) {
                logger.severe("Hot Restart procedure failed", e);
                node.shutdown(true);
            }
        }

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
    }

    private void initWanConsumers() {
        WanReplicationService wanReplicationService = node.nodeEngine.getWanReplicationService();
        if (wanReplicationService != null && wanReplicationService instanceof EnterpriseWanReplicationService) {
            ((EnterpriseWanReplicationService) wanReplicationService).initializeCustomConsumers();
        }
    }

    @Override
    public boolean isStartCompleted() {
        if (hotRestartService != null) {
            return hotRestartService.isStartCompleted();
        }
        return super.isStartCompleted();
    }

    public License getLicense() {
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
                    .setRollingUpgradeEnabled(node.isRollingUpgradeEnabled())
                    .build();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        return ss;
    }

    private void createMemoryManager(Node node) {
        final NativeMemoryConfig memoryConfig = node.getConfig().getNativeMemoryConfig();

        if (memoryConfig.isEnabled()) {
            MemorySize size = memoryConfig.getSize();
            NativeMemoryConfig.MemoryAllocatorType type = memoryConfig.getAllocatorType();
            final FreeMemoryChecker freeMemoryChecker = new FreeMemoryChecker(node.getProperties());

            logger.info("Creating " + type + " native memory manager with " + size.toPrettyString() + " size");
            if (type == NativeMemoryConfig.MemoryAllocatorType.STANDARD) {
                if (isHotRestartEnabled()) {
                    throw new ConfigurationException("MemoryAllocatorType.STANDARD cannot be used when Hot Restart "
                            + "is enabled. Please use MemoryAllocatorType.POOLED!");
                }
                memoryManager = new StandardMemoryManager(size, freeMemoryChecker);
            } else {
                int blockSize = memoryConfig.getMinBlockSize();
                int pageSize = memoryConfig.getPageSize();
                float metadataSpace = memoryConfig.getMetadataSpacePercentage();
                memoryManager = isHotRestartEnabled()
                        ? new HotRestartPoolingMemoryManager(size, blockSize, pageSize, metadataSpace, freeMemoryChecker)
                        : new PoolingMemoryManager(size, blockSize, pageSize, metadataSpace, freeMemoryChecker);
            }
        }
    }

    @Override
    public SecurityContext getSecurityContext() {
        return securityContext;
    }

    @Override
    public MemberSocketInterceptor getMemberSocketInterceptor() {
        return memberSocketInterceptor;
    }

    @Override
    public SocketChannelWrapperFactory getSocketChannelWrapperFactory() {
        final NetworkConfig networkConfig = node.config.getNetworkConfig();
        SSLConfig sslConfig = networkConfig.getSSLConfig();
        if (sslConfig != null && sslConfig.isEnabled()) {
            SymmetricEncryptionConfig symmetricEncryptionConfig = networkConfig.getSymmetricEncryptionConfig();
            if (symmetricEncryptionConfig != null && symmetricEncryptionConfig.isEnabled()) {
                throw new RuntimeException("SSL and SymmetricEncryption cannot be both enabled!");
            }
            logger.info("SSL is enabled");
            return new SSLSocketChannelWrapperFactory(sslConfig);
        }
        return super.getSocketChannelWrapperFactory();
    }

    @Override
    public ReadHandler createReadHandler(final TcpIpConnection connection, final IOService ioService) {
        final NetworkConfig networkConfig = node.config.getNetworkConfig();
        final SymmetricEncryptionConfig symmetricEncryptionConfig = networkConfig.getSymmetricEncryptionConfig();

        if (symmetricEncryptionConfig != null && symmetricEncryptionConfig.isEnabled()) {
            logger.info("Reader started with SymmetricEncryption");
            return new SymmetricCipherMemberReadHandler(connection, ioService, node.nodeEngine.getPacketDispatcher());
        }
        return super.createReadHandler(connection, ioService);
    }

    @Override
    public WriteHandler createWriteHandler(final TcpIpConnection connection, final IOService ioService) {
        final NetworkConfig networkConfig = node.config.getNetworkConfig();
        final SymmetricEncryptionConfig symmetricEncryptionConfig = networkConfig.getSymmetricEncryptionConfig();

        if (symmetricEncryptionConfig != null && symmetricEncryptionConfig.isEnabled()) {
            logger.info("Writer started with SymmetricEncryption");
            return new SymmetricCipherMemberWriteHandler(connection, ioService);
        }
        return super.createWriteHandler(connection, ioService);
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

    public HotRestartService getHotRestartService() {
        if (hotRestartService == null) {
            throw new HotRestartException("HotRestart is not enabled!");
        }
        return hotRestartService;
    }

    public boolean isHotRestartEnabled() {
        return hotRestartService != null;
    }

    public boolean isFeatureEnabledForLicenseKey(Feature feature) {
        boolean enabled = true;
        try {
            LicenseHelper.checkLicenseKeyPerFeature(license.getKey(),
                    buildInfo.getVersion(), feature);
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
                LicenseHelper.checkLicenseKeyPerFeature(license.getKey(),
                        buildInfo.getVersion(), Feature.WAN);
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
        Map<String, Object> services = super.createExtensionServices();
        if (hotRestartService == null) {
            return services;
        }

        if (services.isEmpty()) {
            services = Collections.<String, Object>singletonMap(HotRestartService.SERVICE_NAME, hotRestartService);
        } else {
            services.put(HotRestartService.SERVICE_NAME, hotRestartService);
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
        validateJoiningMemberVersion(joinRequest);
        NativeMemoryConfig memoryConfig = node.getConfig().getNativeMemoryConfig();
        if (!memoryConfig.isEnabled()) {
            return;
        }
        //3.5 license keys have limited HD so we check available hd memory.
        if (license.getVersion() == LicenseVersion.V2) {
            long totalNativeMemorySize = node.getClusterService().getSize()
                    * memoryConfig.getSize().bytes();
            long licensedNativeMemorySize = MemoryUnit.GIGABYTES.toBytes(license.getAllowedNativeMemorySize());
            if (totalNativeMemorySize >= licensedNativeMemorySize) {
                throw new InvalidLicenseException("Total native memory of cluster exceeds licensed native memory. "
                        + "Please contact sales@hazelcast.com");
            }
        }
        license = LicenseHelper.checkLicenseKeyPerFeature(license.getKey(), buildInfo.getVersion(),
                Feature.HD_MEMORY);
    }

    // validate that the joining member is at same major and >= minor version as the cluster version at which this cluster
    // operates
    private void validateJoiningMemberVersion(JoinMessage joinMessage) {
        ClusterVersion clusterVersion = node.getClusterService().getClusterVersion();

        if (clusterVersion.getMajor() != joinMessage.getVersion().getMajor()
                || clusterVersion.getMinor() > joinMessage.getVersion().getMinor()) {
            throw new VersionMismatchException("Joining node's version " + joinMessage.getVersion() + " is not compatible "
                    + "with cluster version " + clusterVersion);
        }
    }

    /**
     * Check if this node's version is compatible with given cluster version. For rolling upgrades context, a node's version
     * is considered compatible with cluster of same version or if cluster's minor version number is smaller by one versus
     * node's minor version number.
     *
     * @param clusterVersion the cluster version to check against
     * @return {@code true} if compatible, otherwise false.
     */
    @Override
    public boolean isNodeVersionCompatibleWith(ClusterVersion clusterVersion) {
        Preconditions.checkNotNull(clusterVersion);

        // node can either work at its codebase version (native mode)
        // or at a minor version that's smaller by one (emulated mode)
        return (node.getVersion().getMajor() == clusterVersion.getMajor()
                && (node.getVersion().getMinor() == clusterVersion.getMinor()
                    || node.getVersion().getMinor() - clusterVersion.getMinor() == 1));
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
    public void onClusterVersionChange(ClusterVersion newVersion) {
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
    public boolean triggerForceStart() {
        if (hotRestartService == null) {
            logger.warning("Ignoring force start request, hot restart is not enabled!");
            return false;
        }

        return hotRestartService.triggerForceStart();
    }

    @Override
    public boolean triggerPartialStart() {
        if (hotRestartService == null) {
            logger.warning("Ignoring partial start request, hot restart is not enabled!");
            return false;
        }

        return hotRestartService.triggerPartialStart();
    }

    @Override
    public String createMemberUuid(Address address) {
        if (hotRestartService != null) {
            String uuid = hotRestartService.getClusterMetadataManager().readMemberUuid();
            if (uuid != null) {
                return uuid;
            }
        }
        return super.createMemberUuid(address);
    }

    @Override
    public boolean isMemberExcluded(Address memberAddress, String memberUuid) {
        if (hotRestartService != null) {
            Set<String> excludedMemberUuids = hotRestartService.getClusterMetadataManager().getExcludedMemberUuids();
            return excludedMemberUuids.contains(memberUuid);
        }
        return super.isMemberExcluded(memberAddress, memberUuid);
    }

    @Override
    public Set<String> getExcludedMemberUuids() {
        if (hotRestartService != null) {
            return hotRestartService.getClusterMetadataManager().getExcludedMemberUuids();
        }

        return super.getExcludedMemberUuids();
    }

    @Override
    public void handleExcludedMemberUuids(Address sender, Set<String> excludedMemberUuids) {
        if (!excludedMemberUuids.contains(node.getThisUuid())) {
            logger.warning("Should handle final cluster start result with excluded member uuids: " + excludedMemberUuids
                    + " within hot restart service since this member is not excluded. sender: " + sender);
            return;
        }

        if (hotRestartService != null) {
            ClusterMetadataManager clusterMetadataManager = hotRestartService.getClusterMetadataManager();
            clusterMetadataManager.receiveHotRestartStatus(sender, CLUSTER_START_SUCCEEDED, excludedMemberUuids, null);
        }

        super.handleExcludedMemberUuids(sender, excludedMemberUuids);
    }

    @Override
    public ClusterHotRestartStatusDTO getCurrentClusterHotRestartStatus() {
        if (hotRestartService != null) {
            ClusterMetadataManager clusterMetadataManager = hotRestartService.getClusterMetadataManager();
            return ClusterHotRestartStatusDTOUtil.create(clusterMetadataManager);
        }
        return super.getCurrentClusterHotRestartStatus();
    }

    @Override
    public void resetHotRestartData() {
        if (hotRestartService != null) {
            hotRestartService.resetHotRestartData();
        }
    }
}
