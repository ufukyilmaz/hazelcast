package com.hazelcast.client.impl.clientside;

import com.hazelcast.client.HazelcastClientNotActiveException;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.config.SocketOptions;
import com.hazelcast.client.impl.spi.ClientExecutionService;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.memory.impl.LibMallocFactory;
import com.hazelcast.internal.memory.impl.PersistentMemoryMallocFactory;
import com.hazelcast.internal.memory.impl.UnsafeMallocFactory;
import com.hazelcast.internal.metrics.StaticMetricsProvider;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.nearcache.EnterpriseNearCacheManager;
import com.hazelcast.internal.nearcache.NearCacheManager;
import com.hazelcast.internal.networking.ChannelInitializer;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.EnterpriseClusterVersionAware;
import com.hazelcast.internal.serialization.impl.EnterpriseSerializationServiceBuilder;
import com.hazelcast.internal.memory.FreeMemoryChecker;
import com.hazelcast.internal.memory.HazelcastMemoryManager;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.internal.memory.MemoryStats;
import com.hazelcast.internal.memory.PoolingMemoryManager;
import com.hazelcast.internal.memory.StandardMemoryManager;
import com.hazelcast.nio.SocketInterceptor;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.partition.PartitioningStrategy;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.version.Version;

import java.util.concurrent.Executor;

/**
 * Enterprise implementation of {@code ClientExtension}.
 */
public class EnterpriseClientExtension extends DefaultClientExtension implements StaticMetricsProvider {

    private final BuildInfo buildInfo = BuildInfoProvider.getBuildInfo();
    private final EnterpriseClientVersionAware versionAware = new EnterpriseClientVersionAware(buildInfo.getVersion());

    private HazelcastMemoryManager memoryManager;

    @Override
    public void beforeStart(HazelcastClientInstanceImpl client) {
        super.beforeStart(client);
    }

    @Override
    public InternalSerializationService createSerializationService(byte version) {
        InternalSerializationService ss;
        try {
            ClientConfig config = client.getClientConfig();
            ClassLoader configClassLoader = config.getClassLoader();

            PartitioningStrategy partitioningStrategy = getPartitioningStrategy(configClassLoader);

            EnterpriseSerializationServiceBuilder builder = new EnterpriseSerializationServiceBuilder();
            SerializationConfig serializationConfig = config.getSerializationConfig() != null ? config
                    .getSerializationConfig() : new SerializationConfig();
            memoryManager = getMemoryManager(client);
            ss = builder.setMemoryManager(memoryManager)
                    .setClassLoader(configClassLoader)
                    .setConfig(serializationConfig)
                    .setManagedContext(new HazelcastClientManagedContext(client, config.getManagedContext()))
                    .setPartitioningStrategy(partitioningStrategy)
                    .setHazelcastInstance(client)
                    .setClusterVersionAware(versionAware)
                    // the client doesn't use the versioned serialization
                    .setVersionedSerializationEnabled(false)
                    .setNotActiveExceptionSupplier(() -> new HazelcastClientNotActiveException("Client is shutdown"))
                    .build();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        return ss;
    }

    private HazelcastMemoryManager getMemoryManager(HazelcastClientInstanceImpl client) {
        final NativeMemoryConfig memoryConfig = client.getClientConfig().getNativeMemoryConfig();

        if (memoryConfig.isEnabled()) {
            MemorySize size = memoryConfig.getSize();
            NativeMemoryConfig.MemoryAllocatorType type = memoryConfig.getAllocatorType();
            final FreeMemoryChecker freeMemoryChecker = new FreeMemoryChecker(client.getProperties());
            String persistentMemoryDirectory = memoryConfig.getPersistentMemoryDirectory();
            final LibMallocFactory libMallocFactory = persistentMemoryDirectory == null
                    ?  new UnsafeMallocFactory(freeMemoryChecker)
                    : new PersistentMemoryMallocFactory(memoryConfig);

            LOGGER.info("Creating " + type + " native memory manager with " + size.toPrettyString() + " size");
            if (type == NativeMemoryConfig.MemoryAllocatorType.STANDARD) {
                return new StandardMemoryManager(size, libMallocFactory);
            } else {
                int blockSize = memoryConfig.getMinBlockSize();
                int pageSize = memoryConfig.getPageSize();
                float metadataSpace = memoryConfig.getMetadataSpacePercentage();
                return new PoolingMemoryManager(size, blockSize, pageSize, metadataSpace, libMallocFactory);
            }
        }
        return null;
    }

    @Override
    public ChannelInitializer createChannelInitializer() {
        ClientNetworkConfig networkConfig = client.getClientConfig().getNetworkConfig();
        SSLConfig sslConfig = networkConfig.getSSLConfig();
        return createChannelInitializer(sslConfig, networkConfig.getSocketOptions());
    }

    @Override
    public ChannelInitializer createChannelInitializer(SSLConfig sslConfig, SocketOptions socketOptions) {
        if (sslConfig != null && sslConfig.isEnabled()) {
            LOGGER.info("SSL is enabled");
            Executor executor = client.getClientExecutionService().getUserExecutor();
            return new ClientTLSChannelInitializer(sslConfig, executor, socketOptions);
        }
        return super.createChannelInitializer(sslConfig, socketOptions);
    }

    @Override
    public SocketInterceptor createSocketInterceptor() {
        ClientNetworkConfig networkConfig = client.getClientConfig().getNetworkConfig();
        return createSocketInterceptor(networkConfig.getSocketInterceptorConfig());
    }

    @Override
    public SocketInterceptor createSocketInterceptor(SocketInterceptorConfig sic) {
        SocketInterceptor implementation = null;
        if (sic != null && sic.isEnabled()) {
            implementation = (SocketInterceptor) sic.getImplementation();
            if (implementation == null && sic.getClassName() != null) {
                try {
                    implementation = (SocketInterceptor) Class.forName(sic.getClassName()).newInstance();
                } catch (Throwable e) {
                    LOGGER.severe("SocketInterceptor class cannot be instantiated!" + sic.getClassName(), e);
                }
            }
        }

        if (implementation != null) {
            implementation.init(sic.getProperties());
        }
        return implementation;
    }

    @Override
    public NearCacheManager createNearCacheManager() {
        SerializationService ss = client.getSerializationService();
        ClientExecutionService es = client.getClientExecutionService();
        ClassLoader classLoader = client.getClientConfig().getClassLoader();
        HazelcastProperties properties = client.getProperties();

        return new EnterpriseNearCacheManager(((EnterpriseSerializationService) ss), es, classLoader, properties);
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
    public MemoryStats getMemoryStats() {
        HazelcastMemoryManager mm = memoryManager;
        return mm != null ? mm.getMemoryStats() : super.getMemoryStats();
    }

    private static class EnterpriseClientVersionAware implements EnterpriseClusterVersionAware {

        private final Version version;

        EnterpriseClientVersionAware(String buildVersion) {
            this.version = Version.of(buildVersion);
        }

        @Override
        public Version getClusterVersion() {
            return version;
        }
    }
}
