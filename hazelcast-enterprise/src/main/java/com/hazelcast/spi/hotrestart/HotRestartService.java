package com.hazelcast.spi.hotrestart;

import com.hazelcast.cluster.ClusterService;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.HotRestartPersistenceConfig;
import com.hazelcast.instance.EnterpriseNodeExtension;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeState;
import com.hazelcast.logging.ILogger;
import com.hazelcast.memory.MemoryManager;
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
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationexecutor.OperationExecutor;
import com.hazelcast.spi.impl.operationexecutor.classic.OperationThread;
import com.hazelcast.spi.impl.operationexecutor.classic.PartitionOperationThread;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cluster.impl.ClusterStateManagerAccessor.setClusterState;
import static com.hazelcast.nio.IOUtil.toFileName;
import static com.hazelcast.spi.hotrestart.PersistentCacheDescriptors.toPartitionId;
import static com.hazelcast.spi.hotrestart.cluster.HotRestartClusterInitializationStatus.FORCE_STARTED;
import static com.hazelcast.spi.hotrestart.cluster.HotRestartClusterInitializationStatus.VERIFICATION_AND_LOAD_SUCCEEDED;
import static com.hazelcast.spi.hotrestart.impl.HotRestartStoreImpl.newOffHeapHotRestartStore;
import static com.hazelcast.spi.hotrestart.impl.HotRestartStoreImpl.newOnHeapHotRestartStore;

/**
 * Provides common services needed for Hot Restart.
 * HotRestartService is main integration point between HotRestart infrastructure
 * and Hazelcast services. It manages RamStoreRegistry(s), is access point for
 * per thread on-heap and off-heap HotRestart stores. Also, it's listener for
 * membership and cluster state events.
 */
@SuppressWarnings({"checkstyle:classfanoutcomplexity" })
public class HotRestartService implements RamStoreRegistry, MembershipAwareService {

    /** Name of the Hot Restart service */
    public static final String SERVICE_NAME = "hz:ee:hotRestartService";

    private static final String STORE_PREFIX = "s";
    private static final String ONHEAP_SUFFIX = "0";
    private static final String OFFHEAP_SUFFIX = "1";
    private static final String STORE_NAME_PATTERN = '^' + STORE_PREFIX + "\\d+" + ONHEAP_SUFFIX + '$';

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

    private final long dataLoadTimeoutMillis;

    public HotRestartService(Node node) {
        this.node = node;
        this.logger = node.getLogger(getClass());
        final Address adr = node.getThisAddress();
        HotRestartPersistenceConfig hotRestartPersistenceConfig = node.getConfig().getHotRestartPersistenceConfig();
        hotRestartHome = new File(hotRestartPersistenceConfig.getBaseDir(), toFileName(adr.getHost() + '-' + adr.getPort()));
        clusterMetadataManager = new ClusterMetadataManager(node, hotRestartHome, hotRestartPersistenceConfig);
        persistentCacheDescriptors = new PersistentCacheDescriptors(hotRestartHome);
        dataLoadTimeoutMillis = TimeUnit.SECONDS.toMillis(hotRestartPersistenceConfig.getDataLoadTimeoutSeconds());
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

    private File getStoreDir(int storeId, boolean onheap) {
        return new File(hotRestartHome, STORE_PREFIX + storeId + (onheap ? ONHEAP_SUFFIX : OFFHEAP_SUFFIX));
    }

    public HotRestartStore getOnHeapHotRestartStoreForCurrentThread() {
        return onHeapStoreHolder.get();
    }

    public HotRestartStore getOffHeapHotRestartStoreForCurrentThread() {
        return offHeapStoreHolder.get();
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

    public <C> C getProvisionalConfiguration(String serviceName, String name) {
        return (C) persistentCacheDescriptors.getProvisionalConfig(serviceName, name);
    }

    public String getCacheName(long prefix) {
        final CacheDescriptor descriptor = persistentCacheDescriptors.getDescriptor(prefix);
        if (descriptor == null) {
            throw new IllegalArgumentException("No descriptor found for prefix: " + prefix);
        }
        return descriptor.getName();
    }

    public void prepare() {
        OperationExecutor operationExecutor = getOperationExecutor();
        final int threadCount = operationExecutor.getPartitionOperationThreadCount();
        final int lastNumberOfHotRestartStores = findNumberOfHotRestartStoreDirectories();
        if (lastNumberOfHotRestartStores != 0 && threadCount != lastNumberOfHotRestartStores) {
            throw new HotRestartException("Number of Hot Restart stores is changed before restart! "
                    + "Current number: " + lastNumberOfHotRestartStores
                    + ", Requested number: " + threadCount);
        }

        createHotRestartStores();
        clusterMetadataManager.prepare();
    }

    private int findNumberOfHotRestartStoreDirectories() {
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

    public void start() {
        try {
            logger.info("Starting hot-restart service...");
            clusterMetadataManager.start();
            persistentCacheDescriptors.restore(node.getSerializationService());

            OperationExecutor opExec = getOperationExecutor();
            final CountDownLatch doneLatch = new CountDownLatch(opExec.getPartitionOperationThreadCount());
            final List<Throwable> exceptions = Collections.synchronizedList(new ArrayList<Throwable>());

            boolean allowData = clusterMetadataManager.isStartWithHotRestart();
            if (allowData) {
                logger.info("Starting hot-restart data load process.");
            } else {
                logger.info("Initializing hot-restart stores, not expecting to load any data.");
            }

            long start = Clock.currentTimeMillis();
            opExec.runOnAllPartitionThreads(new PartitionedLoader(exceptions, doneLatch));

            Throwable failure = awaitDataLoadTasks(doneLatch);
            persistentCacheDescriptors.clearProvisionalConfigs();
            if (failure == null && !exceptions.isEmpty()) {
                failure = exceptions.get(0);
            }
            clusterMetadataManager.loadCompletedLocal(failure);
            logger.info("Hot-restart data load completed in "
                    + TimeUnit.MILLISECONDS.toSeconds(Clock.currentTimeMillis() - start) + " seconds.");
        } catch (ForceStartException e) {
            handleForceStart();
        } catch (Throwable e) {
            logger.severe("Hot-restart failed!", e);
            throw ExceptionUtil.rethrow(e);
        }
    }

    private Throwable awaitDataLoadTasks(CountDownLatch doneLatch) throws InterruptedException {
        // Waiting in steps to check node state.
        // It can be that, node is already shutdown
        // and tasks scheduled in partition threads are discarded.
        // If that happens, latch never be count-down.
        final long awaitStep = 1000;
        long timeout = dataLoadTimeoutMillis;
        boolean done = false;
        while (timeout > 0 && !done) {
            if (clusterMetadataManager.getHotRestartStatus() == FORCE_STARTED) {
                throw new ForceStartException();
            }

            done = doneLatch.await(awaitStep, TimeUnit.MILLISECONDS);
            if (node.getState() == NodeState.SHUT_DOWN) {
                return new HotRestartException("Node is already shutdown!");
            }
            timeout -= awaitStep;
        }
        return done ? null : new HotRestartException("Hot-restart data load timed-out!");
    }

    private void handleForceStart() {
        logger.warning("Force start requested! Skipping hot restart...");

        OperationExecutor operationExecutor = getOperationExecutor();
        logger.fine("Interrupting partition threads...");
        operationExecutor.interruptAllPartitionThreads();

        logger.fine("Closing hot restart stores...");
        closeHotRestartStores();

        logger.info("Deleting hot restart base-dir: " + hotRestartHome);
        IOUtil.delete(hotRestartHome);

        logger.fine("Resetting all services...");
        NodeEngineImpl nodeEngine = node.getNodeEngine();
        Collection<ManagedService> services = nodeEngine.getServices(ManagedService.class);
        for (ManagedService service : services) {
            if (service instanceof ClusterService) {
                continue;
            }
            logger.fine("Resetting service: " + service);
            service.reset();
        }

        logger.fine("Resetting NodeEngine...");
        node.nodeEngine.reset();

        logger.fine("Resetting hot restart cluster metadata...");
        clusterMetadataManager.reset();

        logger.fine("Creating thread local hot restart stores...");
        createHotRestartStores();

        logger.fine("Resetting cluster state to ACTIVE...");
        setClusterState(node.getClusterService(), ClusterState.ACTIVE, true);

        logger.info("Force start completed.");
    }

    private void createThreadLocalHotRestartStores(Thread thread, MemoryManager memoryManager) {
        if (!(thread instanceof PartitionOperationThread)) {
            throw new IllegalArgumentException("PartitionOperationThread is required! -> " + thread);
        }

        final int threadId = ((OperationThread) thread).getThreadId();

        final HotRestartStoreConfig cfg = new HotRestartStoreConfig();
        cfg.setRamStoreRegistry(this)
                .setLoggingService(node.loggingService)
                .setMetricsRegistry(node.nodeEngine.getMetricsRegistry());
        final HotRestartStore onHeapStore =
                newOnHeapHotRestartStore(cfg.setHomeDir(getStoreDir(threadId, true)));
        onHeapStoreHolder.set(onHeapStore);

        if (memoryManager != null) {
            final HotRestartStore offHeapStore =
                    newOffHeapHotRestartStore(cfg.setHomeDir(getStoreDir(threadId, false))
                            .setMalloc(memoryManager.unwrapMemoryAllocator()));
            offHeapStoreHolder.set(offHeapStore);
        }
    }

    private void createHotRestartStores() {
        final MemoryManager memoryManager = ((EnterpriseNodeExtension) node.getNodeExtension())
                .getMemoryManager();
        OperationExecutor operationExecutor = getOperationExecutor();
        CountDownLatch latch = new CountDownLatch(operationExecutor.getPartitionOperationThreadCount());
        operationExecutor.runOnAllPartitionThreads(new CreateHotRestartStoresTask(latch, memoryManager));

        try {
            latch.await();
        } catch (InterruptedException e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    public boolean triggerForceStart() {
        final InternalOperationService operationService = node.nodeEngine.getOperationService();

        final Address masterAddress = node.getMasterAddress();

        if (node.isMaster()) {
            return clusterMetadataManager.receiveForceStartTrigger(node.getThisAddress());
        } else if (masterAddress != null) {
            return operationService.send(new TriggerForceStartOnMasterOperation(), masterAddress);
        } else {
            logger.warning("force start is not triggered since there is no master");
            return false;
        }
    }

    private OperationExecutor getOperationExecutor() {
        NodeEngineImpl nodeEngine = node.getNodeEngine();
        OperationServiceImpl operationService = (OperationServiceImpl) nodeEngine.getOperationService();
        return operationService.getOperationExecutor();
    }

    public void shutdown() {
        clusterMetadataManager.shutdown();
        closeHotRestartStores();
    }

    private void closeHotRestartStores() {
        OperationExecutor operationExecutor = getOperationExecutor();
        final CountDownLatch latch = new CountDownLatch(operationExecutor.getPartitionOperationThreadCount());
        operationExecutor.runOnAllPartitionThreads(new CloseHotRestartStoresTask(latch));

        try {
            latch.await();
        } catch (InterruptedException e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    public boolean isStartCompleted() {
        final HotRestartClusterInitializationStatus status = clusterMetadataManager.getHotRestartStatus();
        return status == VERIFICATION_AND_LOAD_SUCCEEDED || status == FORCE_STARTED;
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

    private class PartitionedLoader extends PartitionedTask {
        private final List<Throwable> exceptions;

        PartitionedLoader(List<Throwable> exceptions, CountDownLatch doneLatch) {
            super(doneLatch);
            this.exceptions = exceptions;
        }

        @Override
        protected void innerRun() throws Exception {
            boolean allowData = clusterMetadataManager.isStartWithHotRestart();
            onHeapStoreHolder.get().hotRestart(!allowData);

            HotRestartStore hotRestartStore = offHeapStoreHolder.get();
            if (hotRestartStore != null) {
                hotRestartStore.hotRestart(!allowData);
            }
        }

        @Override
        protected void onError(Throwable e) {
            exceptions.add(e);
        }
    }

    private class CreateHotRestartStoresTask extends PartitionedTask {

        private final MemoryManager memoryManager;

        CreateHotRestartStoresTask(CountDownLatch latch, MemoryManager memoryManager) {
            super(latch);
            this.memoryManager = memoryManager;
        }

        @Override
        protected void innerRun() throws Exception {
            createThreadLocalHotRestartStores(Thread.currentThread(), memoryManager);
        }

        @Override
        protected void onError(Throwable e) {
            logger.severe("While creating Hot Restart stores...", e);
        }
    }

    private class CloseHotRestartStoresTask extends PartitionedTask {
        CloseHotRestartStoresTask(CountDownLatch latch) {
            super(latch);
        }

        @Override
        protected void innerRun() throws Exception {
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

        private void closeStore(HotRestartStore hotRestartStore) {
            try {
                hotRestartStore.close();
            } catch (Throwable e) {
                logger.severe(e);
            }
        }

        @Override
        protected void onError(Throwable e) {
            logger.severe("While closing Hot Restart stores...", e);
        }
    }

    private abstract class PartitionedTask implements Runnable {
        final CountDownLatch doneLatch;

        protected PartitionedTask(CountDownLatch doneLatch) {
            this.doneLatch = doneLatch;
        }

        @Override
        public final void run() {
            try {
                innerRun();
            } catch (Throwable e) {
                onError(e);
            } finally {
                doneLatch.countDown();
            }
        }

        protected abstract void innerRun() throws Exception;

        protected abstract void onError(Throwable e);
    }
}
