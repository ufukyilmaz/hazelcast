package com.hazelcast.client.impl;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.map.impl.proxy.EnterpriseMapClientProxyFactory;
import com.hazelcast.client.spi.ClientProxyFactory;
import com.hazelcast.client.spi.impl.ClientExecutionServiceImpl;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.nearcache.HiDensityNearCacheManager;
import com.hazelcast.internal.nearcache.NearCacheManager;
import com.hazelcast.internal.networking.SocketChannelWrapperFactory;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.EnterpriseClusterVersionAware;
import com.hazelcast.internal.serialization.impl.EnterpriseSerializationServiceBuilder;
import com.hazelcast.license.domain.Feature;
import com.hazelcast.license.domain.License;
import com.hazelcast.license.domain.LicenseVersion;
import com.hazelcast.license.util.LicenseHelper;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.memory.FreeMemoryChecker;
import com.hazelcast.memory.HazelcastMemoryManager;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryStats;
import com.hazelcast.memory.PoolingMemoryManager;
import com.hazelcast.memory.StandardMemoryManager;
import com.hazelcast.nio.SocketInterceptor;
import com.hazelcast.version.Version;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.nio.ssl.SSLSocketChannelWrapperFactory;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.ExceptionUtil;

import static com.hazelcast.license.util.LicenseHelper.checkLicenseKeyPerFeature;

/**
 * Enterprise implementation of <tt>ClientExtension</tt>.
 */
public class EnterpriseClientExtension extends DefaultClientExtension {

    private final BuildInfo buildInfo = BuildInfoProvider.getBuildInfo();
    private final EnterpriseClientVersionAware versionAware = new EnterpriseClientVersionAware(buildInfo.getVersion());

    private HazelcastMemoryManager memoryManager;

    private volatile SocketInterceptor socketInterceptor;
    private volatile License license;

    @Override
    public void beforeStart(HazelcastClientInstanceImpl client) {
        super.beforeStart(client);
        ClientConfig clientConfig = client.getClientConfig();
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        initSocketInterceptor(networkConfig.getSocketInterceptorConfig());
        String licenseKey = clientConfig.getLicenseKey();
        if (licenseKey == null) {
            licenseKey = clientConfig.getProperty(GroupProperty.ENTERPRISE_LICENSE_KEY.getName());
        }
        license = LicenseHelper.getLicense(licenseKey, buildInfo.getVersion());
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
                    .build();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        return ss;
    }

    private HazelcastMemoryManager getMemoryManager(HazelcastClientInstanceImpl client) {
        final NativeMemoryConfig memoryConfig = client.getClientConfig().getNativeMemoryConfig();

        if (memoryConfig.isEnabled()) {
            if (license.getVersion() == LicenseVersion.V4) {
                checkLicenseKeyPerFeature(license.getKey(), buildInfo.getVersion(), Feature.HD_MEMORY);
            }
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
    public SocketChannelWrapperFactory createSocketChannelWrapperFactory() {
        final ClientNetworkConfig networkConfig = client.getClientConfig().getNetworkConfig();
        SSLConfig sslConfig = networkConfig.getSSLConfig();
        if (sslConfig != null && sslConfig.isEnabled()) {
            LOGGER.info("SSL is enabled");
            return new SSLSocketChannelWrapperFactory(sslConfig, true);
        }
        return super.createSocketChannelWrapperFactory();
    }

    @Override
    public SocketInterceptor createSocketInterceptor() {
        return socketInterceptor;
    }

    private void initSocketInterceptor(SocketInterceptorConfig sic) {
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
            socketInterceptor = implementation;
        }
    }

    @Override
    public NearCacheManager createNearCacheManager() {
        SerializationService ss = client.getSerializationService();
        ClientExecutionServiceImpl es = client.getExecutionService();
        ClassLoader classLoader = client.getClientConfig().getClassLoader();

        return new HiDensityNearCacheManager(((EnterpriseSerializationService) ss), es, classLoader);
    }

    @Override
    public <T> ClientProxyFactory createServiceProxyFactory(Class<T> service) {
        if (MapService.class.isAssignableFrom(service)) {
            return new EnterpriseMapClientProxyFactory(client.getClientConfig(), client.getProperties());
        }

        throw new IllegalArgumentException("Proxy factory cannot be created. Unknown service : " + service);
    }

    @Override
    public MemoryStats getMemoryStats() {
        HazelcastMemoryManager mm = memoryManager;
        return mm != null ? mm.getMemoryStats() : super.getMemoryStats();
    }

    private static class EnterpriseClientVersionAware implements EnterpriseClusterVersionAware {
        private final Version version;

        public EnterpriseClientVersionAware(String buildVersion) {
            this.version = Version.of(buildVersion);
        }

        @Override
        public Version getClusterVersion() {
            return version;
        }
    }

}
