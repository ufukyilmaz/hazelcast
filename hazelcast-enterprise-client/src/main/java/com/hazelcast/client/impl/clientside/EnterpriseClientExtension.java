package com.hazelcast.client.impl.clientside;

import com.hazelcast.client.HazelcastClientNotActiveException;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.config.SocketOptions;
import com.hazelcast.client.spi.ClientExecutionService;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.metrics.MetricsProvider;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.nearcache.HiDensityNearCacheManager;
import com.hazelcast.internal.nearcache.NearCacheManager;
import com.hazelcast.internal.networking.ChannelInitializer;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.EnterpriseClusterVersionAware;
import com.hazelcast.internal.serialization.impl.EnterpriseSerializationServiceBuilder;
import com.hazelcast.memory.FreeMemoryChecker;
import com.hazelcast.memory.HazelcastMemoryManager;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryStats;
import com.hazelcast.memory.PoolingMemoryManager;
import com.hazelcast.memory.StandardMemoryManager;
import com.hazelcast.nio.SocketInterceptor;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.version.Version;

import java.util.concurrent.Executor;
import java.util.function.Supplier;

/**
 * Enterprise implementation of {@code ClientExtension}.
 */
public class EnterpriseClientExtension extends DefaultClientExtension implements MetricsProvider {

    private final BuildInfo buildInfo = BuildInfoProvider.getBuildInfo();
    private final EnterpriseClientVersionAware versionAware = new EnterpriseClientVersionAware(buildInfo.getVersion());

    private HazelcastMemoryManager memoryManager;

    @Override
    public void beforeStart(HazelcastClientInstanceImpl client) {
        super.beforeStart(client);
        ClientConfig clientConfig = client.getClientConfig();
        String licenseKey = clientConfig.getLicenseKey();
        if (licenseKey == null) {
            licenseKey = clientConfig.getProperty(GroupProperty.ENTERPRISE_LICENSE_KEY.getName());
        }
        if (licenseKey != null) {
            EnterpriseClientExtension.LOGGER.info("As of Hazelcast 3.10.3, "
                    + "enterprise license keys are required only for members, and not for clients.");
        }
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
                    .setNotActiveExceptionSupplier(new Supplier<RuntimeException>() {
                        @Override
                        public RuntimeException get() {
                            return new HazelcastClientNotActiveException("Client is shutdown");
                        }
                    })
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

            LOGGER.info("Creating " + type + " native memory manager with " + size.toPrettyString() + " size");
            if (type == NativeMemoryConfig.MemoryAllocatorType.STANDARD) {
                return new StandardMemoryManager(size, freeMemoryChecker);
            } else {
                int blockSize = memoryConfig.getMinBlockSize();
                int pageSize = memoryConfig.getPageSize();
                float metadataSpace = memoryConfig.getMetadataSpacePercentage();
                return new PoolingMemoryManager(size, blockSize, pageSize, metadataSpace, freeMemoryChecker);
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

        return new HiDensityNearCacheManager(((EnterpriseSerializationService) ss), es, classLoader, properties);
    }

    @Override
    public void provideMetrics(MetricsRegistry registry) {
        if (memoryManager != null) {
            registry.scanAndRegister(memoryManager, "memorymanager");
            if (memoryManager instanceof MetricsProvider) {
                ((MetricsProvider) memoryManager).provideMetrics(registry);
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
