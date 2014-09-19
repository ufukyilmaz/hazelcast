package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.OffHeapMemoryConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.PoolingMemoryManager;
import com.hazelcast.memory.StandardMemoryManager;
import com.hazelcast.nio.SocketInterceptor;
import com.hazelcast.nio.serialization.EnterpriseSerializationServiceBuilder;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.nio.ssl.SSLSocketChannelWrapperFactory;
import com.hazelcast.nio.tcp.SocketChannelWrapperFactory;
import com.hazelcast.util.ExceptionUtil;

/**
 * @author mdogan 05/09/14
 */
public class EnterpriseClientExtension extends DefaultClientExtension {

    private volatile SocketInterceptor socketInterceptor;

    @Override
    public void beforeInitialize(HazelcastClient client) {
        super.beforeInitialize(client);
        ClientConfig clientConfig = client.getClientConfig();
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        initSocketInterceptor(networkConfig.getSocketInterceptorConfig());
    }

    @Override
    public SerializationService createSerializationService() {
        SerializationService ss;
        try {
            ClientConfig config = client.getClientConfig();
            ClassLoader configClassLoader = config.getClassLoader();

            PartitioningStrategy partitioningStrategy = getPartitioningStrategy(configClassLoader);

            EnterpriseSerializationServiceBuilder builder = new EnterpriseSerializationServiceBuilder();
            ss = builder.setMemoryManager(getMemoryManager(config))
                    .setClassLoader(configClassLoader)
                    .setConfig(config.getSerializationConfig() != null ? config.getSerializationConfig() : new SerializationConfig())
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
        OffHeapMemoryConfig memoryConfig = config.getOffHeapMemoryConfig();
        if (memoryConfig.isEnabled()) {
            MemorySize size = memoryConfig.getSize();
            OffHeapMemoryConfig.MemoryAllocatorType type = memoryConfig.getAllocatorType();
            LOGGER.info("Creating " + type + " offheap memory manager with " + size.toPrettyString() + " size");
            if (type == OffHeapMemoryConfig.MemoryAllocatorType.STANDARD) {
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
    public SocketChannelWrapperFactory getSocketChannelWrapperFactory() {
        final ClientNetworkConfig networkConfig = client.getClientConfig().getNetworkConfig();
        SSLConfig sslConfig = networkConfig.getSSLConfig();
        if (sslConfig != null && sslConfig.isEnabled()) {
            LOGGER.info("SSL is enabled");
            return new SSLSocketChannelWrapperFactory(sslConfig);
        }
        return super.getSocketChannelWrapperFactory();
    }

    @Override
    public SocketInterceptor getSocketInterceptor() {
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
}
