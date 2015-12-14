package com.hazelcast.instance;

import com.hazelcast.cache.EnterpriseCacheService;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.client.impl.protocol.EnterpriseMessageTaskFactoryImpl;
import com.hazelcast.client.impl.protocol.MessageTaskFactory;
import com.hazelcast.client.impl.protocol.MessageTaskFactoryImpl;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigurationException;
import com.hazelcast.config.HotRestartConfig;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.EnterpriseSerializationServiceBuilder;
import com.hazelcast.license.domain.License;
import com.hazelcast.license.domain.LicenseType;
import com.hazelcast.license.exception.InvalidLicenseException;
import com.hazelcast.license.util.LicenseHelper;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryStats;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.memory.PoolingMemoryManager;
import com.hazelcast.memory.StandardMemoryManager;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.MemberSocketInterceptor;
import com.hazelcast.nio.SocketInterceptor;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.nio.ssl.SSLSocketChannelWrapperFactory;
import com.hazelcast.nio.tcp.ReadHandler;
import com.hazelcast.nio.tcp.SocketChannelWrapperFactory;
import com.hazelcast.nio.tcp.SymmetricCipherMemberReadHandler;
import com.hazelcast.nio.tcp.SymmetricCipherMemberWriteHandler;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.nio.tcp.WriteHandler;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.security.SecurityContextImpl;
import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.hotrestart.HotRestartService;
import com.hazelcast.spi.hotrestart.cluster.ClusterHotRestartEventListener;
import com.hazelcast.spi.impl.operationexecutor.classic.PartitionOperationThread;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.wan.WanReplicationService;
import com.hazelcast.wan.impl.WanReplicationServiceImpl;

import java.util.Collections;
import java.util.Map;
import java.util.logging.Level;

import static com.hazelcast.map.impl.EnterpriseMapServiceConstructor.getEnterpriseMapServiceConstructor;

/**
 * This class is the enterprise system hook to allow injection of enterprise services into Hazelcast subsystems
 */
@SuppressWarnings({"checkstyle:classdataabstractioncoupling", "checkstyle:methodcount" })
public class EnterpriseNodeExtension extends DefaultNodeExtension implements NodeExtension {

    private final HotRestartService hotRestartService;
    private volatile License license;
    private volatile SecurityContext securityContext;
    private volatile MemberSocketInterceptor memberSocketInterceptor;
    private volatile MemoryManager memoryManager;

    public EnterpriseNodeExtension(Node node) {
        super(node);
        hotRestartService = createHotRestartService(node);
    }

    private HotRestartService createHotRestartService(Node node) {
        HotRestartConfig hotRestartConfig = node.getConfig().getHotRestartConfig();
        return hotRestartConfig.isEnabled() ? new HotRestartService(node) : null;
    }

    @Override
    public void beforeStart() {
        logger.log(Level.INFO, "Checking Hazelcast Enterprise license...");

        String licenseKey = node.groupProperties.getString(GroupProperty.ENTERPRISE_LICENSE_KEY);
        if (licenseKey == null || "".equals(licenseKey)) {
            licenseKey = node.getConfig().getLicenseKey();
        }
        final BuildInfo buildInfo = BuildInfoProvider.getBuildInfo();
        license = LicenseHelper.checkLicenseKey(licenseKey, buildInfo.getVersion(),
                LicenseType.ENTERPRISE, LicenseType.ENTERPRISE_SECURITY_ONLY);
        logger.log(Level.INFO, license.toString());

        createSecurityContext(node);
        createMemoryManager(node.config);
        createSocketInterceptor(node.config.getNetworkConfig());
    }

    private void createSecurityContext(Node node) {
        boolean securityEnabled = node.getConfig().getSecurityConfig().isEnabled();
        if (securityEnabled) {
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
            hotRestartService.prepare();
        }
    }

    @Override
    public void printNodeInfo() {
        BuildInfo buildInfo = node.getBuildInfo();
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
        final int count = node.getClusterService().getSize();
        if (count > license.getAllowedNumberOfNodes()) {
            logger.log(Level.SEVERE,
                    "Exceeded maximum number of nodes allowed in Hazelcast Enterprise license! Max: "
                            + license.getAllowedNumberOfNodes() + ", Current: " + count);
            node.shutdown(true);
        }

        if (hotRestartService != null) {
            try {
                hotRestartService.start();
            } catch (Throwable e) {
                logger.severe("Hot-restart failed!", e);
                node.shutdown(true);
            }
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
    public SerializationService createSerializationService() {
        SerializationService ss;
        try {
            Config config = node.getConfig();
            ClassLoader configClassLoader = node.getConfigClassLoader();

            HazelcastInstanceImpl hazelcastInstance = node.hazelcastInstance;
            PartitioningStrategy partitioningStrategy = getPartitioningStrategy(configClassLoader);

            EnterpriseSerializationServiceBuilder builder = new EnterpriseSerializationServiceBuilder();
            SerializationConfig serializationConfig = config.getSerializationConfig() != null ? config
                    .getSerializationConfig() : new SerializationConfig();

            ss = builder
                    .setMemoryManager(memoryManager)
                    .setClassLoader(configClassLoader)
                    .setConfig(serializationConfig)
                    .setManagedContext(hazelcastInstance.managedContext)
                    .setPartitioningStrategy(partitioningStrategy)
                    .setHazelcastInstance(hazelcastInstance)
                    .build();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        return ss;
    }

    private void createMemoryManager(Config config) {
        NativeMemoryConfig memoryConfig = config.getNativeMemoryConfig();
        if (memoryConfig.isEnabled()) {
            MemorySize size = memoryConfig.getSize();
            NativeMemoryConfig.MemoryAllocatorType type = memoryConfig.getAllocatorType();
            logger.info("Creating " + type + " native memory manager with " + size.toPrettyString() + " size");
            if (type == NativeMemoryConfig.MemoryAllocatorType.STANDARD) {
                if (isHotRestartEnabled()) {
                    throw new ConfigurationException("MemoryAllocatorType.STANDARD cannot be used when Hot Restart "
                            + "is enabled. Please use MemoryAllocatorType.POOLED!");
                }
                memoryManager = new StandardMemoryManager(size);
            } else {
                int blockSize = memoryConfig.getMinBlockSize();
                int pageSize = memoryConfig.getPageSize();
                float metadataSpace = memoryConfig.getMetadataSpacePercentage();
                memoryManager = new PoolingMemoryManager(size, blockSize, pageSize, metadataSpace);
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
    public MessageTaskFactory createMessageTaskFactory() {
        EnterpriseMessageTaskFactoryImpl enterprise = new EnterpriseMessageTaskFactoryImpl(node);
        MessageTaskFactoryImpl messageTaskFactory = (MessageTaskFactoryImpl) super.createMessageTaskFactory();

        MessageTaskFactory[] communityTasks = messageTaskFactory.getTasks();
        MessageTaskFactory[] enterpriseTasks = enterprise.getTasks();
        for (int i = 0; i < communityTasks.length; i++) {
            MessageTaskFactory task = communityTasks[i];
            if (task != null) {
                enterpriseTasks[i] = task;
            }
        }

        return enterprise;
    }

    @Override
    public void onThreadStart(Thread thread) {
        registerThreadToPoolingMemoryManager(thread);
        registerThreadToHotRestart(thread);
    }

    private void registerThreadToHotRestart(Thread thread) {
        if (!(thread instanceof PartitionOperationThread)) {
            return;
        }

        if (hotRestartService != null) {
            hotRestartService.registerThread(thread, memoryManager);
        }
    }

    private void registerThreadToPoolingMemoryManager(Thread thread) {
        if (!(thread instanceof PartitionOperationThread)) {
            return;
        }
        EnterpriseSerializationService serializationService
                = (EnterpriseSerializationService) node.getSerializationService();

        MemoryManager memoryManager = serializationService.getMemoryManager();
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

        MemoryManager memoryManager = serializationService.getMemoryManager();
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
            if (license.getType() == LicenseType.ENTERPRISE_SECURITY_ONLY) {
                return (T) new WanReplicationServiceImpl(node);
            } else {
                return (T) new EnterpriseWanReplicationService(node);
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
        MemoryManager mm = memoryManager;
        return mm != null ? mm.getMemoryStats() : super.getMemoryStats();
    }

    @Override
    public void validateJoinRequest() {
        NativeMemoryConfig memoryConfig = node.getConfig().getNativeMemoryConfig();
        if (!memoryConfig.isEnabled()) {
            return;
        }
        final BuildInfo buildInfo = BuildInfoProvider.getBuildInfo();
        license = LicenseHelper.checkLicenseKey(license.getKey(), buildInfo.getVersion(),
                LicenseType.ENTERPRISE);
        long totalNativeMemorySize = node.getClusterService().getSize()
                * memoryConfig.getSize().bytes();
        long licensedNativeMemorySize = MemoryUnit.GIGABYTES.toBytes(license.getAllowedNativeMemorySize());
        if (totalNativeMemorySize >= licensedNativeMemorySize) {
            throw new InvalidLicenseException("Total native memory of cluster exceeds licensed native memory. "
                    + "Please contact sales@hazelcast.com");
        }
    }

    @Override
    public void onClusterStateChange(ClusterState newState) {
        if (hotRestartService != null) {
            hotRestartService.getClusterMetadataManager().onClusterStateChange(newState);
        }
    }

    @Override
    public boolean registerListener(Object listener) {
        if (listener instanceof ClusterHotRestartEventListener) {
            if (listener instanceof HazelcastInstanceAware) {
                ((HazelcastInstanceAware) listener).setHazelcastInstance(node.hazelcastInstance);
            }

            if (hotRestartService == null) {
                throw new HotRestartException("HotRestart is not enabled!");
            }
            hotRestartService.addClusterHotRestartEventListener((ClusterHotRestartEventListener) listener);
            return true;
        }
        return false;
    }
}
