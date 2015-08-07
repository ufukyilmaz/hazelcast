package com.hazelcast.instance;

import com.hazelcast.cache.EnterpriseCacheService;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.client.impl.protocol.EnterpriseMessageTaskFactoryImpl;
import com.hazelcast.client.impl.protocol.MessageTaskFactory;
import com.hazelcast.client.impl.protocol.MessageTaskFactoryImpl;
import com.hazelcast.config.Config;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.elasticmemory.InstanceStorageFactory;
import com.hazelcast.elasticmemory.SingletonStorageFactory;
import com.hazelcast.elasticmemory.StorageFactory;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.internal.storage.Storage;
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
import com.hazelcast.nio.serialization.impl.EnterpriseSerializationServiceBuilder;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.nio.ssl.SSLSocketChannelWrapperFactory;
import com.hazelcast.nio.tcp.PacketReader;
import com.hazelcast.nio.tcp.PacketWriter;
import com.hazelcast.nio.tcp.SocketChannelWrapperFactory;
import com.hazelcast.nio.tcp.SymmetricCipherPacketReader;
import com.hazelcast.nio.tcp.SymmetricCipherPacketWriter;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.security.SecurityContextImpl;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.wan.WanReplicationService;
import com.hazelcast.wan.impl.WanReplicationServiceImpl;

import java.util.logging.Level;

import static com.hazelcast.map.impl.EnterpriseMapServiceConstructor.getEnterpriseMapServiceConstructor;

/**
 * This class is the enterprise system hook to allow injection of enterprise services into Hazelcast subsystems
 */
public class EnterpriseNodeExtension extends DefaultNodeExtension implements NodeExtension {

    private volatile Storage storage;
    private volatile License license;
    private volatile SecurityContext securityContext;
    private volatile MemberSocketInterceptor memberSocketInterceptor;
    private volatile MemoryManager memoryManager;

    public EnterpriseNodeExtension() {
        super();
    }

    @Override
    public void beforeStart(Node node) {
        this.node = node;
        logger = node.getLogger("com.hazelcast.enterprise.initializer");
        logger.log(Level.INFO, "Checking Hazelcast Enterprise license...");

        String licenseKey = node.groupProperties.ENTERPRISE_LICENSE_KEY.getString();
        if (licenseKey == null || "".equals(licenseKey)) {
            licenseKey = node.getConfig().getLicenseKey();
        }
        license = LicenseHelper.checkLicenseKey(licenseKey, LicenseType.ENTERPRISE, LicenseType.ENTERPRISE_SECURITY_ONLY);
        logger.log(Level.INFO, license.toString());
        systemLogger = node.getLogger("com.hazelcast.system");

        createSecurityContext(node);
        createMemoryManager(node.config);
        createStorage(node);
        createSocketInterceptor(node.config.getNetworkConfig());
    }

    private void createSecurityContext(Node node) {
        boolean securityEnabled = node.getConfig().getSecurityConfig().isEnabled();
        if (securityEnabled) {
            securityContext = new SecurityContextImpl(node);
        }
    }

    private void createStorage(Node node) {
        if (node.groupProperties.ELASTIC_MEMORY_ENABLED.getBoolean()) {
            StorageFactory storageFactory;
            if (node.groupProperties.ELASTIC_MEMORY_SHARED_STORAGE.getBoolean()) {
                logger.log(Level.WARNING, "Using SingletonStorageFactory for Hazelcast Elastic Memory...");
                storageFactory = new SingletonStorageFactory();
            } else {
                storageFactory = new InstanceStorageFactory(node);
            }

            logger.log(Level.INFO, "Initializing node off-heap storage.");
            storage = storageFactory.createStorage();
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
    public void printNodeInfo(Node node) {
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
    public void afterStart(Node node) {
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
    public PacketReader createPacketReader(final TcpIpConnection connection, final IOService ioService) {
        final NetworkConfig networkConfig = node.config.getNetworkConfig();
        final SymmetricEncryptionConfig symmetricEncryptionConfig = networkConfig.getSymmetricEncryptionConfig();

        if (symmetricEncryptionConfig != null && symmetricEncryptionConfig.isEnabled()) {
            logger.info("Reader started with SymmetricEncryption");
            return new SymmetricCipherPacketReader(connection, ioService, node.nodeEngine.getPacketTransceiver());
        }
        return super.createPacketReader(connection, ioService);
    }

    @Override
    public PacketWriter createPacketWriter(final TcpIpConnection connection, final IOService ioService) {
        final NetworkConfig networkConfig = node.config.getNetworkConfig();
        final SymmetricEncryptionConfig symmetricEncryptionConfig = networkConfig.getSymmetricEncryptionConfig();

        if (symmetricEncryptionConfig != null && symmetricEncryptionConfig.isEnabled()) {
            logger.info("Writer started with SymmetricEncryption");
            return new SymmetricCipherPacketWriter(connection, ioService);
        }
        return super.createPacketWriter(connection, ioService);
    }

    @Override
    public MessageTaskFactory createMessageTaskFactory(Node node) {
        EnterpriseMessageTaskFactoryImpl enterprise = new EnterpriseMessageTaskFactoryImpl(node);
        MessageTaskFactoryImpl messageTaskFactory = (MessageTaskFactoryImpl) super.createMessageTaskFactory(node);

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
    public Storage getNativeDataStorage() {
        if (storage == null) {
            throw new IllegalStateException(
                    "Offheap storage is not enabled! " + "Please set 'hazelcast.elastic.memory.enabled' to true");
        }
        return storage;
    }

    @Override
    public void onThreadStart(Thread thread) {
        EnterpriseSerializationService serializationService
                = (EnterpriseSerializationService) node.getSerializationService();

        MemoryManager memoryManager = serializationService.getMemoryManager();
        if (memoryManager instanceof PoolingMemoryManager) {
            ((PoolingMemoryManager) memoryManager).registerThread(thread);
        }
    }

    @Override
    public void onThreadStop(Thread thread) {
        EnterpriseSerializationService serializationService
                = (EnterpriseSerializationService) node.getSerializationService();

        MemoryManager memoryManager = serializationService.getMemoryManager();
        if (memoryManager instanceof PoolingMemoryManager) {
            ((PoolingMemoryManager) memoryManager).deregisterThread(thread);
        }
    }

    @Override
    public void destroy() {
        super.destroy();
        license = null;
        if (storage != null) {
            logger.log(Level.FINEST, "Destroying node off-heap storage.");
            storage.destroy();
            storage = null;
        }
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
    public MemoryStats getMemoryStats() {
        MemoryManager mm = memoryManager;
        return mm != null ? mm.getMemoryStats() : super.getMemoryStats();
    }

    @Override
    public void beforeJoin() {
        NativeMemoryConfig memoryConfig = node.getConfig().getNativeMemoryConfig();
        if (!memoryConfig.isEnabled()) {
            return;
        }
        LicenseHelper.checkLicenseKey(license.getKey(), LicenseType.ENTERPRISE);
        long totalNativeMemorySize = node.getClusterService().getSize()
                * memoryConfig.getSize().bytes();
        long licensedNativeMemorySize = MemoryUnit.GIGABYTES.toBytes(license.getAllowedNativeMemorySize());
        if (totalNativeMemorySize >= licensedNativeMemorySize) {
            throw new InvalidLicenseException("Total native memory of cluster exceeds licensed native memory. "
                    + "Please contact sales@hazelcast.com");
        }
    }
}
