package com.hazelcast.client.impl;

import com.hazelcast.cache.hidensity.nearcache.HiDensityNearCacheManager;
import com.hazelcast.cache.impl.nearcache.NearCacheManager;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.license.domain.LicenseType;
import com.hazelcast.license.util.LicenseHelper;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.PoolingMemoryManager;
import com.hazelcast.memory.StandardMemoryManager;
import com.hazelcast.nio.SocketInterceptor;
import com.hazelcast.internal.serialization.impl.EnterpriseSerializationServiceBuilder;
import com.hazelcast.nio.ssl.SSLSocketChannelWrapperFactory;
import com.hazelcast.nio.tcp.SocketChannelWrapperFactory;
import com.hazelcast.util.ExceptionUtil;

/**
 * Enterprise implementation of <tt>ClientExtension</tt>
 */
public class EnterpriseClientExtension extends DefaultClientExtension {

    private volatile SocketInterceptor socketInterceptor;

    @Override
    public void beforeStart(HazelcastClientInstanceImpl client) {
        super.beforeStart(client);
        ClientConfig clientConfig = client.getClientConfig();
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        initSocketInterceptor(networkConfig.getSocketInterceptorConfig());
        String licenseKey = clientConfig.getLicenseKey();
        if (licenseKey == null) {
            licenseKey = clientConfig.getProperty(GroupProperties.PROP_ENTERPRISE_LICENSE_KEY);
        }
        LicenseHelper.checkLicenseKey(licenseKey, LicenseType.ENTERPRISE,
                LicenseType.ENTERPRISE_SECURITY_ONLY);
    }

    @Override
    public SerializationService createSerializationService() {
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

    private MemoryManager getMemoryManager(ClientConfig config) {
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
    public <T> Class<? extends ClientProxy> getServiceProxy(Class<T> service) {
        if (MapService.class.isAssignableFrom(service)) {
            return EnterpriseClientMapProxyImpl.class;
        }

        throw new IllegalArgumentException("Unknown service for proxy creation: " + service);
    }

}
