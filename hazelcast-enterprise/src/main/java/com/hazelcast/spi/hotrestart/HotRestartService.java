package com.hazelcast.spi.hotrestart;

import com.hazelcast.config.HotRestartConfig;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.MemberAttributeServiceEvent;
import com.hazelcast.spi.MembershipAwareService;
import com.hazelcast.spi.MembershipServiceEvent;
import com.hazelcast.spi.hotrestart.cluster.ClusterHotRestartEventListener;
import com.hazelcast.spi.hotrestart.cluster.ClusterMetadataManager;
import com.hazelcast.spi.hotrestart.cluster.HotRestartClusterInitializationStatus;
import com.hazelcast.spi.hotrestart.impl.HotRestartStoreConfig;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationexecutor.OperationExecutor;
import com.hazelcast.spi.impl.operationexecutor.classic.OperationThread;
import com.hazelcast.spi.impl.operationexecutor.classic.PartitionOperationThread;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.util.ExceptionUtil;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.spi.hotrestart.PersistentCacheDescriptors.toFileName;
import static com.hazelcast.spi.hotrestart.PersistentCacheDescriptors.toPartitionId;
import static com.hazelcast.spi.hotrestart.impl.HotRestartStoreImpl.newOffHeapHotRestartStore;
import static com.hazelcast.spi.hotrestart.impl.HotRestartStoreImpl.newOnHeapHotRestartStore;

/**
 * Provides common services needed for Hot Restart.
 * HotRestartService is main integration point between HotRestart infrastructure
 * and Hazelcast services. It manages RamStoreRegistry(s), is access point for
 * per thread on-heap and off-heap HotRestart stores. Also, it's listener for
 * membership and cluster state events.
 */
public class HotRestartService implements RamStoreRegistry, MembershipAwareService {

    /** Name of the Hot Restart service */
    public static final String SERVICE_NAME = "hz:ee:hotRestartService";

    private static final String SEGMENT_PREFIX = "s";
    private static final String ONHEAP_SUFFIX = "0";
    private static final String OFFHEAP_SUFFIX = "1";

    @SuppressWarnings("ThreadLocalNotStaticFinal")
    private final ThreadLocal<HotRestartStore> onHeapStoreHolder = new ThreadLocal<HotRestartStore>();

    @SuppressWarnings("ThreadLocalNotStaticFinal")
    private final ThreadLocal<HotRestartStore> offHeapStoreHolder = new ThreadLocal<HotRestartStore>();

    private final Map<String, RamStoreRegistry> ramStoreRegistryMap = new ConcurrentHashMap<String, RamStoreRegistry>();

    private final Map<Long, RamStoreDescriptor> ramStoreDescriptors = new ConcurrentHashMap<Long, RamStoreDescriptor>();

    private final File hotRestartHome;

    private final Node node;

    private final ILogger logger;

    private final PersistentCacheDescriptors persistentCacheDescriptors;

    private final ClusterMetadataManager clusterMetadataManager;

    public HotRestartService(Node node) {
        this.node = node;
        this.logger = node.getLogger(getClass());
        final Address adr = node.getThisAddress();
        HotRestartConfig hotRestartConfig = node.getConfig().getHotRestartConfig();
        hotRestartHome = new File(hotRestartConfig.getHomeDir(), toFileName(adr.getHost() + '-' + adr.getPort()));
        clusterMetadataManager = new ClusterMetadataManager(node, hotRestartHome, hotRestartConfig);
        persistentCacheDescriptors = new PersistentCacheDescriptors(hotRestartHome);
    }

    public void addClusterHotRestartEventListener(final ClusterHotRestartEventListener listener) {
        this.clusterMetadataManager.addClusterHotRestartEventListener(listener);
    }

    @Override
    public RamStore ramStoreForPrefix(long prefix) {
        RamStoreDescriptor descriptor = getRamStoreDescriptor(prefix);
        return descriptor.registry.ramStoreForPrefix(prefix);
    }

    private RamStoreDescriptor getRamStoreDescriptor(long prefix) {
        RamStoreDescriptor descriptor = ramStoreDescriptors.get(prefix);
        if (descriptor == null) {
            descriptor = constructRamStoreDescriptorFromCacheDescriptor(prefix);
        }
        if (descriptor == null) {
            throw new IllegalArgumentException("No registration available for: " + prefix);
        }
        return descriptor;
    }

    private RamStoreDescriptor constructRamStoreDescriptorFromCacheDescriptor(long prefix) {
        CacheDescriptor cacheDescriptor = persistentCacheDescriptors.getDescriptor(prefix);
        if (cacheDescriptor != null) {
            String serviceName = cacheDescriptor.getServiceName();
            RamStoreRegistry registry = ramStoreRegistryMap.get(serviceName);
            if (registry != null) {
                String name = cacheDescriptor.getName();
                int partitionId = toPartitionId(prefix);
                RamStoreDescriptor descriptor = new RamStoreDescriptor(registry, name, partitionId);
                ramStoreDescriptors.put(prefix, descriptor);
                return descriptor;
            }
        }
        return null;
    }

    @Override
    public RamStore restartingRamStoreForPrefix(long prefix) {
        RamStoreDescriptor descriptor = getRamStoreDescriptor(prefix);
        return descriptor.registry.restartingRamStoreForPrefix(prefix);
    }

    public void registerThread(Thread thread, MemoryManager memoryManager) {
        if (!(thread instanceof PartitionOperationThread)) {
            throw new IllegalArgumentException("PartitionOperationThread is required! -> " + thread);
        }

        String segment = SEGMENT_PREFIX + ((OperationThread) thread).getThreadId();

        final HotRestartStoreConfig cfg = new HotRestartStoreConfig();
        cfg.setRamStoreRegistry(this).setLoggingService(node.loggingService);
        final HotRestartStore onHeapStore =
                newOnHeapHotRestartStore(cfg.setHomeDir(new File(hotRestartHome, segment + ONHEAP_SUFFIX)));
        //todo: Replace auto-fsync with a good fsync policy
        onHeapStore.setAutoFsync(true);
        onHeapStoreHolder.set(onHeapStore);

        if (memoryManager != null) {
            final HotRestartStore offHeapStore =
                    newOffHeapHotRestartStore(cfg.setHomeDir(new File(hotRestartHome, segment + OFFHEAP_SUFFIX))
                                                 .setMalloc(memoryManager.unwrapMemoryAllocator()));
            //todo: Replace auto-fsync with a good fsync policy
            offHeapStore.setAutoFsync(true);
            offHeapStoreHolder.set(offHeapStore);
        }
    }

    private void closeStore(HotRestartStore hotRestartStore) {
        try {
            hotRestartStore.close();
        } catch (Throwable e) {
            logger.severe(e);
        }
    }

    public HotRestartStore getOnHeapHotRestartStoreForCurrentThread() {
        return onHeapStoreHolder.get();
    }

    public HotRestartStore getOffHeapHotRestartStoreForCurrentThread() {
        return offHeapStoreHolder.get();
    }

    public long registerRamStore(RamStoreRegistry ramStoreRegistry, String name, int partitionId) {
        long prefix = persistentCacheDescriptors.getPrefix(name, partitionId);
        ramStoreDescriptors.put(prefix, new RamStoreDescriptor(ramStoreRegistry, name, partitionId));
        return prefix;
    }

    public void registerRamStoreRegistry(String serviceName, RamStoreRegistry registry) {
        ramStoreRegistryMap.put(serviceName, registry);
    }

    public void ensureHasConfiguration(String serviceName, String name, Object config) {
        persistentCacheDescriptors.ensureHas(node.getSerializationService(), serviceName, name, config);
    }

    public Object getProvisionalConfiguration(String serviceName, String name) {
        return persistentCacheDescriptors.getProvisionalConfig(serviceName, name);
    }

    public String getCacheName(long prefix) {
        final CacheDescriptor descriptor = persistentCacheDescriptors.getDescriptor(prefix);
        if (descriptor == null) {
            throw new IllegalArgumentException("No descriptor found for prefix: " + prefix);
        }
        return descriptor.getName();
    }

    public String getProvisionalCacheName(long prefix) {
        final CacheDescriptor descriptor = persistentCacheDescriptors.getDescriptor(prefix);
        if (descriptor == null) {
            throw new IllegalArgumentException("No descriptor found for prefix: " + prefix);
        }
        return descriptor.getProvisionalName();
    }

    public void prepare() {
        clusterMetadataManager.prepare();
    }

    public void start() {
        logger.info("Starting hot-restart service...");
        clusterMetadataManager.start();
        persistentCacheDescriptors.restore(node.getSerializationService());

        OperationExecutor opExec = getOperationExecutor();
        final CountDownLatch doneLatch = new CountDownLatch(opExec.getPartitionOperationThreadCount());
        final List<Throwable> exceptions = Collections.synchronizedList(new ArrayList<Throwable>());

        logger.info("Starting hot-restart data load process.");

        opExec.runOnAllPartitionThreads(new PartitionedLoader(exceptions, doneLatch));

        try {
            doneLatch.await();
            persistentCacheDescriptors.clearProvisionalConfigs();
            Throwable failure = null;
            if (!exceptions.isEmpty()) {
                failure = exceptions.get(0);
            }
            clusterMetadataManager.loadCompletedLocal(failure);
            logger.info("Hot-restart data load completed.");
        } catch (Throwable e) {
            logger.severe("Hot-restart failed!", e);
            throw ExceptionUtil.rethrow(e);
        }
    }

    private OperationExecutor getOperationExecutor() {
        NodeEngineImpl nodeEngine = node.getNodeEngine();
        OperationServiceImpl operationService = (OperationServiceImpl) nodeEngine.getOperationService();
        return operationService.getOperationExecutor();
    }

    public void shutdown() {
        OperationExecutor operationExecutor = getOperationExecutor();
        operationExecutor.runOnAllPartitionThreads(new Runnable() {
            @Override
            public void run() {
                HotRestartStore hotRestartStore = onHeapStoreHolder.get();
                onHeapStoreHolder.remove();
                if (hotRestartStore != null) {
                    closeStore(hotRestartStore);
                }

                hotRestartStore = offHeapStoreHolder.get();
                offHeapStoreHolder.remove();
                if (hotRestartStore != null) {
                    closeStore(hotRestartStore);
                }
            }
        });
    }

    public boolean isStartCompleted() {
        final HotRestartClusterInitializationStatus status = clusterMetadataManager.getHotRestartStatus();
        return status == HotRestartClusterInitializationStatus.VERIFICATION_AND_LOAD_SUCCEEDED;
    }

    private static class RamStoreDescriptor {
        final RamStoreRegistry registry;
        final String name;
        final int partitionId;

        RamStoreDescriptor(RamStoreRegistry registry, String name, int partitionId) {
            this.registry = registry;
            this.name = name;
            this.partitionId = partitionId;
        }
    }

    @Override
    public void memberAdded(MembershipServiceEvent event) {
        clusterMetadataManager.onMembershipChange(event);
    }

    @Override
    public void memberRemoved(MembershipServiceEvent event) {
        clusterMetadataManager.onMembershipChange(event);
    }

    @Override
    public void memberAttributeChanged(MemberAttributeServiceEvent event) {
    }

    public ClusterMetadataManager getClusterMetadataManager() {
        return clusterMetadataManager;
    }

    private class PartitionedLoader implements Runnable {
        private final List<Throwable> exceptions;
        private final CountDownLatch doneLatch;

        public PartitionedLoader(List<Throwable> exceptions, CountDownLatch doneLatch) {
            this.exceptions = exceptions;
            this.doneLatch = doneLatch;
        }

        @Override
        public void run() {
            try {
                boolean allowData = clusterMetadataManager.isStartWithHotRestart();
                onHeapStoreHolder.get().hotRestart(!allowData);

                HotRestartStore hotRestartStore = offHeapStoreHolder.get();
                if (hotRestartStore != null) {
                    hotRestartStore.hotRestart(!allowData);
                }
            } catch (Throwable e) {
                exceptions.add(e);
            } finally {
                doneLatch.countDown();
            }
        }
    }
}
