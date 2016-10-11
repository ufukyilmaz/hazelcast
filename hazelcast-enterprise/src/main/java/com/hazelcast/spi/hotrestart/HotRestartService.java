package com.hazelcast.spi.hotrestart;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.HotRestartPersistenceConfig;
import com.hazelcast.instance.EnterpriseNodeExtension;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeState;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.memory.HazelcastMemoryManager;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MemberAttributeServiceEvent;
import com.hazelcast.spi.MembershipAwareService;
import com.hazelcast.spi.MembershipServiceEvent;
import com.hazelcast.spi.hotrestart.cluster.ClusterHotRestartEventListener;
import com.hazelcast.spi.hotrestart.cluster.ClusterMetadataManager;
import com.hazelcast.spi.hotrestart.cluster.HotRestartClusterInitializationStatus;
import com.hazelcast.spi.hotrestart.cluster.TriggerForceStartOnMasterOperation;
import com.hazelcast.spi.hotrestart.impl.HotRestartStoreConfig;
import com.hazelcast.spi.hotrestart.impl.RamStoreRestartLoop;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationexecutor.OperationExecutor;
import com.hazelcast.spi.impl.operationexecutor.impl.OperationThread;
import com.hazelcast.spi.impl.operationexecutor.impl.PartitionOperationThread;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.cluster.impl.ClusterStateManagerAccessor.setClusterState;
import static com.hazelcast.nio.IOUtil.toFileName;
import static com.hazelcast.spi.hotrestart.PersistentCacheDescriptors.toPartitionId;
import static com.hazelcast.spi.hotrestart.cluster.HotRestartClusterInitializationStatus.FORCE_STARTED;
import static com.hazelcast.spi.hotrestart.cluster.HotRestartClusterInitializationStatus.VERIFICATION_AND_LOAD_SUCCEEDED;
import static com.hazelcast.spi.hotrestart.impl.HotRestartModule.newOffHeapHotRestartStore;
import static com.hazelcast.spi.hotrestart.impl.HotRestartModule.newOnHeapHotRestartStore;
import static com.hazelcast.util.Clock.currentTimeMillis;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Provides common services needed for Hot Restart.
 * HotRestartService is main integration point between HotRestart infrastructure
 * and Hazelcast services. It manages RamStoreRegistry(s), is access point for
 * per thread on-heap and off-heap HotRestart stores. Also, it's listener for
 * membership and cluster state events.
 */
@SuppressWarnings({ "checkstyle:classfanoutcomplexity", "checkstyle:methodcount", "checkstyle:classdataabstractioncoupling" })
public class HotRestartService implements RamStoreRegistry, MembershipAwareService {

    /**
     * Name of the Hot Restart service
     */
    public static final String SERVICE_NAME = "hz:ee:hotRestartService";

    private static final char STORE_PREFIX = 's';
    private static final char ONHEAP_SUFFIX = '0';
    private static final char OFFHEAP_SUFFIX = '1';
    private static final String STORE_NAME_PATTERN = STORE_PREFIX + "\\d+" + ONHEAP_SUFFIX;

    private final Map<String, RamStoreRegistry> ramStoreRegistryMap = new ConcurrentHashMap<String, RamStoreRegistry>();
    private final Map<Long, RamStoreDescriptor> ramStoreDescriptors = new ConcurrentHashMap<Long, RamStoreDescriptor>();
    private final File hotRestartHome;
    private final Node node;
    private final ILogger logger;
    private final PersistentCacheDescriptors persistentCacheDescriptors;
    private final ClusterMetadataManager clusterMetadataManager;
    private final long dataLoadTimeoutMillis;
    private final int storeCount;
    private HotRestartStore[] onHeapStores;
    private HotRestartStore[] offHeapStores;
    private int partitionThreadCount;
    private final List<LoadedConfigurationListener> loadedConfigurationListeners;

    public HotRestartService(Node node) {
        this.node = node;
        this.logger = node.getLogger(getClass());
        final Address adr = node.getThisAddress();
        final HotRestartPersistenceConfig hrCfg = node.getConfig().getHotRestartPersistenceConfig();
        this.hotRestartHome = new File(hrCfg.getBaseDir(), toFileName(adr.getHost() + '-' + adr.getPort()));
        this.storeCount = hrCfg.getParallelism();
        this.clusterMetadataManager = new ClusterMetadataManager(node, hotRestartHome, hrCfg);
        this.persistentCacheDescriptors = new PersistentCacheDescriptors(hotRestartHome);
        this.dataLoadTimeoutMillis = TimeUnit.SECONDS.toMillis(hrCfg.getDataLoadTimeoutSeconds());
        this.loadedConfigurationListeners = new ArrayList<LoadedConfigurationListener>();
    }

    public void registerLoadedConfigurationListener(LoadedConfigurationListener listener) {
        loadedConfigurationListeners.add(listener);
    }

    @Override
    public RamStore ramStoreForPrefix(long prefix) {
        return ramStoreDescriptor(prefix).registry.ramStoreForPrefix(prefix);
    }

    @Override
    public RamStore restartingRamStoreForPrefix(long prefix) {
        return ramStoreDescriptor(prefix).registry.restartingRamStoreForPrefix(prefix);
    }

    @Override
    public int prefixToThreadId(long prefix) {
        return toPartitionId(prefix) % partitionThreadCount;
    }

    public HotRestartStore getOnHeapHotRestartStoreForCurrentThread() {
        return onHeapStores[storeIndexForCurrentThread()];
    }

    public HotRestartStore getOffHeapHotRestartStoreForCurrentThread() {
        return offHeapStores[storeIndexForCurrentThread()];
    }

    public long registerRamStore(RamStoreRegistry ramStoreRegistry, String serviceName, String name, int partitionId) {
        long prefix = persistentCacheDescriptors.getPrefix(serviceName, name, partitionId);
        ramStoreDescriptors.put(prefix, new RamStoreDescriptor(ramStoreRegistry, name, partitionId));
        return prefix;
    }

    public void registerRamStoreRegistry(String serviceName, RamStoreRegistry registry) {
        ramStoreRegistryMap.put(serviceName, registry);
    }

    public void ensureHasConfiguration(String serviceName, String name, Object config) {
        persistentCacheDescriptors.ensureHas(node.getSerializationService(), serviceName, name, config);
    }

    public String getCacheName(long prefix) {
        final CacheDescriptor descriptor = persistentCacheDescriptors.getDescriptor(prefix);
        if (descriptor == null) {
            throw new IllegalArgumentException("No descriptor found for prefix: " + prefix);
        }
        return descriptor.getName();
    }

    @Override
    public void memberAdded(MembershipServiceEvent event) {
        clusterMetadataManager.onMembershipChange();
    }

    @Override
    public void memberRemoved(MembershipServiceEvent event) {
        clusterMetadataManager.onMembershipChange();
    }

    @Override
    public void memberAttributeChanged(MemberAttributeServiceEvent event) { }

    public void addClusterHotRestartEventListener(final ClusterHotRestartEventListener listener) {
        this.clusterMetadataManager.addClusterHotRestartEventListener(listener);
    }

    public ClusterMetadataManager getClusterMetadataManager() {
        return clusterMetadataManager;
    }

    public void prepare() {
        partitionThreadCount = getOperationExecutor().getPartitionThreadCount();
        final int persistedStoreCount = persistedStoreCount();
        if (persistedStoreCount > 0) {
            if (storeCount != persistedStoreCount) {
                throw new HotRestartException(String.format(
                        "Mismatch between configured and actual level of parallelism in Hot Restart Persistence."
                        + " Configured %d, actual %d",
                        storeCount, persistedStoreCount));
            }
            final int persistedPartitionThreadCount = clusterMetadataManager.readPartitionThreadCount();
            if (partitionThreadCount != persistedPartitionThreadCount) {
                throw new HotRestartException(String.format(
                        "Mismatch between the current number of partition operation threads and"
                        + " the number persisted in the Hot Restart data. Current %d, persisted %d",
                        partitionThreadCount, persistedPartitionThreadCount));
            }
        } else {
            if (storeCount <= 0) {
                throw new HotRestartException("Configured Hot Restart store count must be a positive integer, but is "
                        + storeCount);
            }
            clusterMetadataManager.writePartitionThreadCount(partitionThreadCount);
        }
        clusterMetadataManager.prepare();
        createHotRestartStores();
    }

    public void start() {
        try {
            logger.info("Starting hot-restart service...");
            clusterMetadataManager.start();
            persistentCacheDescriptors.restore(node.getSerializationService(), loadedConfigurationListeners);
            boolean allowData = clusterMetadataManager.isStartWithHotRestart();
            logger.info(allowData ? "Starting the Hot Restart procedure."
                                  : "Initializing Hot Restart stores, not expecting to reload any data.");
            long start = currentTimeMillis();
            Throwable failure = null;
            try {
                runRestarterPipeline(onHeapStores);
                runRestarterPipeline(offHeapStores);
            } catch (ForceStartException e) {
                throw e;
            } catch (Throwable t) {
                failure = t;
            }
            clusterMetadataManager.loadCompletedLocal(failure);
            logger.info(String.format("Hot Restart procedure completed in %,d seconds",
                    MILLISECONDS.toSeconds(currentTimeMillis() - start)));
        } catch (ForceStartException e) {
            handleForceStart();
        } catch (HotRestartException e) {
            throw e;
        } catch (InterruptedException e) {
            currentThread().interrupt();
            throw new HotRestartException("Thread interrupted during the Hot Restart procedure", e);
        } catch (Throwable t) {
            throw new HotRestartException("Hot Restart procedure failed", t);
        }
    }

    public boolean isStartCompleted() {
        final HotRestartClusterInitializationStatus status = clusterMetadataManager.getHotRestartStatus();
        return status == VERIFICATION_AND_LOAD_SUCCEEDED || status == FORCE_STARTED;
    }

    public boolean triggerForceStart() {
        final InternalOperationService operationService = node.nodeEngine.getOperationService();
        final Address masterAddress = node.getMasterAddress();
        if (node.isMaster()) {
            return clusterMetadataManager.receiveForceStartTrigger(node.getThisAddress());
        } else if (masterAddress != null) {
            return operationService.send(new TriggerForceStartOnMasterOperation(), masterAddress);
        } else {
            logger.warning("Force start not triggered because there is no master member");
            return false;
        }
    }

    public void shutdown() {
        clusterMetadataManager.shutdown();
        closeHotRestartStores();
    }

    private int persistedStoreCount() {
        if (!hotRestartHome.exists()) {
            return 0;
        }
        final File[] stores = hotRestartHome.listFiles(new FileFilter() {
            @Override
            public boolean accept(File f) {
                return f.isDirectory() && f.getName().matches(STORE_NAME_PATTERN);
            }
        });
        return stores != null ? stores.length : 0;
    }

    private RamStoreDescriptor ramStoreDescriptor(long prefix) {
        RamStoreDescriptor descriptor = ramStoreDescriptors.get(prefix);
        if (descriptor == null) {
            descriptor = cacheDescriptorToRamStoreDescriptor(prefix);
        }
        if (descriptor == null) {
            throw new IllegalArgumentException("No RamStore registered under prefix " + prefix);
        }
        return descriptor;
    }

    private RamStoreDescriptor cacheDescriptorToRamStoreDescriptor(long prefix) {
        final CacheDescriptor cacheDescriptor = persistentCacheDescriptors.getDescriptor(prefix);
        if (cacheDescriptor != null) {
            final String serviceName = cacheDescriptor.getServiceName();
            final RamStoreRegistry registry = ramStoreRegistryMap.get(serviceName);
            if (registry != null) {
                final String name = cacheDescriptor.getName();
                final int partitionId = toPartitionId(prefix);
                final RamStoreDescriptor descriptor = new RamStoreDescriptor(registry, name, partitionId);
                ramStoreDescriptors.put(prefix, descriptor);
                return descriptor;
            }
        }
        return null;
    }

    private void createHotRestartStores() {
        HazelcastMemoryManager memMgr = ((EnterpriseNodeExtension) node.getNodeExtension()).getMemoryManager();

        onHeapStores = new HotRestartStore[storeCount];
        for (int i = 0; i < storeCount; i++) {
            newHotRestartStoreConfig(i, true);
            onHeapStores[i] = newOnHeapHotRestartStore(newHotRestartStoreConfig(i, true));
        }
        if (memMgr != null) {
            offHeapStores = new HotRestartStore[storeCount];
            for (int i = 0; i < storeCount; i++) {
                offHeapStores[i] = newOffHeapHotRestartStore(
                        newHotRestartStoreConfig(i, false).setMalloc(memMgr.getSystemAllocator()));
            }
        }
    }

    private HotRestartStoreConfig newHotRestartStoreConfig(int storeId, boolean onheap) {
        File dir = storeDir(storeId, onheap);
        String name = node.getHazelcastThreadGroup().getThreadNamePrefix(dir.getName());
        return new HotRestartStoreConfig()
                .setStoreName(name)
                .setHomeDir(dir)
                .setRamStoreRegistry(this)
                .setLoggingService(node.loggingService)
                .setMetricsRegistry(node.nodeEngine.getMetricsRegistry());
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
    private void runRestarterPipeline(HotRestartStore[] stores) throws Throwable {
        if (stores == null) {
            return;
        }
        assert stores.length == storeCount;
        final long deadline = cappedSum(currentTimeMillis(), dataLoadTimeoutMillis);
        final RamStoreRestartLoop loop =
                new RamStoreRestartLoop(stores.length, getOperationExecutor().getPartitionThreadCount(), this, logger);
        final AtomicReference<Throwable> failure = new AtomicReference<Throwable>();
        final Thread[] restartThreads = new Thread[stores.length];
        for (int i = 0; i < stores.length; i++) {
            final int storeIndex = i;
            final HotRestartStore store = stores[storeIndex];
            restartThreads[i] = new Thread(store.name() + ".restart-thread") {
                @Override
                public void run() {
                    try {
                        store.hotRestart(false, storeCount, loop.keyReceivers[storeIndex],
                                loop.keyHandleSenders[storeIndex], loop.valueReceivers[storeIndex]);
                    } catch (Throwable t) {
                        failure.compareAndSet(null, t);
                        logger.severe("Restart thread failed", t);
                    }
                }
            };
        }
        for (Thread t : restartThreads) {
            t.start();
        }
        final CountDownLatch doneLatch = new CountDownLatch(partitionThreadCount);
        getOperationExecutor().executeOnPartitionThreads(new Runnable() {
            @Override public void run() {
                try {
                    loop.run(((OperationThread) currentThread()).getThreadId());
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                } finally {
                    doneLatch.countDown();
                }
            }
        });
        try {
            awaitCompletionOnPartitionThreads(doneLatch, deadline);
        } catch (Throwable t) {
            failure.compareAndSet(null, t);
            for (Thread thr : restartThreads) {
                thr.interrupt();
            }
        }
        for (Thread thr : restartThreads) {
            thr.join(Math.max(1, deadline - currentTimeMillis()));
            if (thr.isAlive()) {
                failure.compareAndSet(null, new HotRestartException("Timed out wating for a restartThread to complete"));
            }
        }
        final Throwable t = failure.get();
        if (t != null) {
            throw t;
        }
    }

    private void awaitCompletionOnPartitionThreads(CountDownLatch doneLatch, long deadline) throws InterruptedException {
        do {
            if (currentTimeMillis() > deadline) {
                throw new HotRestartException("Hot Restart timed out");
            }
            if (node.getState() == NodeState.SHUT_DOWN) {
                throw new HotRestartException("Node is already shut down");
            }
            if (clusterMetadataManager.getHotRestartStatus() == FORCE_STARTED) {
                throw new ForceStartException();
            }
        } while (!doneLatch.await(1, TimeUnit.SECONDS));
    }

    private void handleForceStart() {
        logger.warning("Force start requested, skipping hot restart");
        logger.fine("Closing Hot Restart stores");
        closeHotRestartStores();
        logger.info("Deleting Hot Restart base-dir " + hotRestartHome);
        IOUtil.delete(hotRestartHome);
        logger.fine("Resetting all services");
        NodeEngineImpl nodeEngine = node.getNodeEngine();
        Collection<ManagedService> services = nodeEngine.getServices(ManagedService.class);
        for (ManagedService service : services) {
            if (service instanceof ClusterService) {
                continue;
            }
            logger.fine("Resetting service: " + service);
            service.reset();
        }
        logger.fine("Resetting NodeEngine");
        node.nodeEngine.reset();
        logger.fine("Resetting hot restart cluster metadata");
        clusterMetadataManager.reset();
        clusterMetadataManager.writePartitionThreadCount(getOperationExecutor().getPartitionThreadCount());
        logger.fine("Creating thread local hot restart stores");
        createHotRestartStores();
        logger.fine("Resetting cluster state to ACTIVE");
        setClusterState(node.getClusterService(), ClusterState.ACTIVE, false);
        logger.info("Force start completed");
    }

    private OperationExecutor getOperationExecutor() {
        NodeEngineImpl nodeEngine = node.getNodeEngine();
        OperationServiceImpl operationService = (OperationServiceImpl) nodeEngine.getOperationService();
        return operationService.getOperationExecutor();
    }

    private File storeDir(int storeId, boolean onheap) {
        return new File(hotRestartHome, "" + STORE_PREFIX + storeId + (onheap ? ONHEAP_SUFFIX : OFFHEAP_SUFFIX));
    }

    private int storeIndexForCurrentThread() {
        return ((PartitionOperationThread) currentThread()).getThreadId() % storeCount;
    }

    private void closeHotRestartStores() {
        if (onHeapStores != null) {
            for (HotRestartStore st : onHeapStores) {
                st.close();
            }
        }
        if (offHeapStores != null) {
            for (HotRestartStore st : offHeapStores) {
                st.close();
            }
        }
    }

    private static long cappedSum(long a, long b) {
        assert a >= 0 : "a is negative";
        assert b >= 0 : "b is negative";
        final long sum = a + b;
        return sum >= 0 ? sum : Long.MAX_VALUE;
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
}
