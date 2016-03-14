package com.hazelcast.client.impl;

import com.hazelcast.cache.hidensity.nearcache.HiDensityNearCacheManager;
import com.hazelcast.cache.impl.nearcache.NearCacheManager;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.map.impl.proxy.EnterpriseMapClientProxyFactory;
import com.hazelcast.client.spi.ClientProxyFactory;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.EnterpriseSerializationServiceBuilder;
import com.hazelcast.license.domain.Feature;
import com.hazelcast.license.domain.License;
import com.hazelcast.license.exception.InvalidLicenseException;
import com.hazelcast.license.util.LicenseHelper;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.memory.HazelcastMemoryManager;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.PoolingMemoryManager;
import com.hazelcast.memory.StandardMemoryManager;
import com.hazelcast.nio.SocketInterceptor;
import com.hazelcast.nio.ssl.SSLSocketChannelWrapperFactory;
import com.hazelcast.nio.tcp.SocketChannelWrapperFactory;
import com.hazelcast.util.ExceptionUtil;

/**
 * Enterprise implementation of <tt>ClientExtension</tt>
 */
public class EnterpriseClientExtension extends DefaultClientExtension {

    private volatile SocketInterceptor socketInterceptor;
    private volatile License license;
    private final BuildInfo buildInfo = BuildInfoProvider.getBuildInfo();

    @Override
    public void beforeStart(HazelcastClientInstanceImpl client) {
        super.beforeStart(client);
        ClientConfig clientConfig = client.getClientConfig();
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        initSocketInterceptor(networkConfig.getSocketInterceptorConfig());
        String licenseKey = clientConfig.getLicenseKey();
        if (licenseKey == null) {
            licenseKey = clientConfig.getProperty(GroupProperty.ENTERPRISE_LICENSE_KEY);
        }
        license = LicenseHelper.getLicense(licenseKey, buildInfo.getVersion());
    }

    @Override
    public SerializationService createSerializationService(byte version) {
        SerializationService ss;
        try {
            ClientConfig config = client.getClientConfig();
            ClassLoader configClassLoader = config.getClassLoader();

            PartitioningStrategy partitioningStrategy = getPartitioningStrategy(configClassLoader);

            EnterpriseSerializationServiceBuilder builder = new EnterpriseSerializationServiceBuilder();
            SerializationConfig serializationConfig = config.getSerializationConfig() != null ? config
                    .getSerializationConfig() : new SerializationConfig();
            ss = builder.setMemoryManager(getMemoryManager(config))
                    .setClassLoader(configClassLoader)
                    .setConfig(serializationConfig)
                    .setManagedContext(new HazelcastClientManagedContext(client, config.getManagedContext()))
                    .setPartitioningStrategy(partitioningStrategy)
                    .setHazelcastInstance(client)
                    .build();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        return ss;
    }

    private HazelcastMemoryManager getMemoryManager(ClientConfig config) {
        NativeMemoryConfig memoryConfig = config.getNativeMemoryConfig();
        if (memoryConfig.isEnabled()) {
            MemorySize size = memoryConfig.getSize();
            NativeMemoryConfig.MemoryAllocatorType type = memoryConfig.getAllocatorType();
            LOGGER.info("Creating " + type + " native memory manager with " + size.toPrettyString() + " size");
            if (type == NativeMemoryConfig.MemoryAllocatorType.STANDARD) {
                return new StandardMemoryManager(size);
            } else {
                int blockSize = memoryConfig.getMinBlockSize();
                int pageSize = memoryConfig.getPageSize();
                float metadataSpace = memoryConfig.getMetadataSpacePercentage();
                return new PoolingMemoryManager(size, blockSize, pageSize, metadataSpace);
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
            return new SSLSocketChannelWrapperFactory(sslConfig);
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
        return new HiDensityNearCacheManager();
    }

    @Override
    public <T> ClientProxyFactory createServiceProxyFactory(Class<T> service) {
        if (MapService.class.isAssignableFrom(service)) {
            try {
                LicenseHelper.checkLicenseKeyPerFeature(license.getKey(), buildInfo.getVersion(),
                        Feature.CONTINUOUS_QUERY_CACHE);
                return new EnterpriseMapClientProxyFactory(client.getClientExecutionService(),
                        client.getSerializationService(), client.getClientConfig());
            } catch (InvalidLicenseException e) {
                return super.createServiceProxyFactory(service);
            }
        }

        throw new IllegalArgumentException("Proxy factory cannot be created. Unknown service : " + service);
    }

}
