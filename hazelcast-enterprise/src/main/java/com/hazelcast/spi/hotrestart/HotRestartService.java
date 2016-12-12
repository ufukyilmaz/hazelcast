package com.hazelcast.spi.hotrestart;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.HotRestartPersistenceConfig;
import com.hazelcast.hotrestart.BackupTaskState;
import com.hazelcast.hotrestart.BackupTaskStatus;
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
import com.hazelcast.spi.hotrestart.cluster.HotRestartClusterStartStatus;
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
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.hotrestart.BackupTaskState.FAILURE;
import static com.hazelcast.hotrestart.BackupTaskState.IN_PROGRESS;
import static com.hazelcast.hotrestart.BackupTaskState.SUCCESS;
import static com.hazelcast.hotrestart.HotRestartService.BACKUP_DIR_PREFIX;
import static com.hazelcast.internal.cluster.impl.ClusterStateManagerAccessor.setClusterState;
import static com.hazelcast.spi.hotrestart.PersistentCacheDescriptors.toPartitionId;
import static com.hazelcast.spi.hotrestart.cluster.HotRestartClusterStartStatus.CLUSTER_START_SUCCEEDED;
import static com.hazelcast.spi.hotrestart.impl.HotRestartModule.newOffHeapHotRestartStore;
import static com.hazelcast.spi.hotrestart.impl.HotRestartModule.newOnHeapHotRestartStore;
import static com.hazelcast.util.Clock.currentTimeMillis;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Provides common services needed for Hot Restart.
 * HotRestartService is main integration point between Hot Restart infrastructure
 * and Hazelcast services. It manages RamStoreRegistry(s), is access point for
 * per thread on-heap and off-heap Hot Restart stores. Also, it's listener for
 * membership and cluster state events.
 */
@SuppressWarnings({"checkstyle:classfanoutcomplexity", "checkstyle:methodcount", "checkstyle:classdataabstractioncoupling"})
public class HotRestartService implements RamStoreRegistry, MembershipAwareService {

    /**
     * Name of the Hot Restart service.
     */
    public static final String SERVICE_NAME = "hz:ee:hotRestartService";

    private static final char STORE_PREFIX = 's';
    private static final char ONHEAP_SUFFIX = '0';
    private static final char OFFHEAP_SUFFIX = '1';
    private static final String STORE_NAME_PATTERN = STORE_PREFIX + "\\d+" + ONHEAP_SUFFIX;
    private static final String LOCK_FILE_NAME = "lock";

    private final Map<String, RamStoreRegistry> ramStoreRegistryMap = new ConcurrentHashMap<String, RamStoreRegistry>();
    private final Map<Long, RamStoreDescriptor> ramStoreDescriptors = new ConcurrentHashMap<Long, RamStoreDescriptor>();
    private final File hotRestartHome;
    private final File hotRestartBackupDir;
    private final Node node;
    private final ILogger logger;
    private final PersistentCacheDescriptors persistentCacheDescriptors;
    private final ClusterMetadataManager clusterMetadataManager;
    private final long dataLoadTimeoutMillis;
    private final int storeCount;

    private volatile HotRestartStore[] onHeapStores;
    private volatile HotRestartStore[] offHeapStores;
    private volatile int partitionThreadCount;
    private final List<LoadedConfigurationListener> loadedConfigurationListeners;

    private final DirectoryLock directoryLock;

    public HotRestartService(Node node) {
        this.node = node;
        this.logger = node.getLogger(getClass());
        HotRestartPersistenceConfig hrCfg = node.getConfig().getHotRestartPersistenceConfig();
        this.hotRestartHome = hrCfg.getBaseDir();
        this.hotRestartBackupDir = hrCfg.getBackupDir();
        this.storeCount = hrCfg.getParallelism();
        this.clusterMetadataManager = new ClusterMetadataManager(node, hotRestartHome, hrCfg);
        this.persistentCacheDescriptors = new PersistentCacheDescriptors(hotRestartHome);
        this.dataLoadTimeoutMillis = TimeUnit.SECONDS.toMillis(hrCfg.getDataLoadTimeoutSeconds());
        this.loadedConfigurationListeners = new ArrayList<LoadedConfigurationListener>();
        this.directoryLock = new DirectoryLock();
        logger.fine("Created hot-restart service. Base directory: " + hotRestartHome.getAbsolutePath());
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

    /**
     * Registers a {@link RamStoreRegistry}. There should already be a configuration for the service and distributed object name.
     * This can be ensured by calling {@link #ensureHasConfiguration(String, String, Object)}.
     *
     * @param ramStoreRegistry the registry to be registered
     * @param serviceName      the service name
     * @param name             the distributed object name
     * @param partitionId      the partition ID
     * @return the key (prefix) under which the registry is registered in the hot restart service
     */
    public long registerRamStore(RamStoreRegistry ramStoreRegistry, String serviceName, String name, int partitionId) {
        long prefix = persistentCacheDescriptors.getPrefix(serviceName, name, partitionId);
        ramStoreDescriptors.put(prefix, new RamStoreDescriptor(ramStoreRegistry, name, partitionId));
        return prefix;
    }

    public void registerRamStoreRegistry(String serviceName, RamStoreRegistry registry) {
        ramStoreRegistryMap.put(serviceName, registry);
    }

    /**
     * Ensures that the configuration exists for the given service name and distributed object name. Creates one if there is none.
     *
     * @param serviceName the service name
     * @param name        the distributed object name
     * @param config      the configuration
     */
    public void ensureHasConfiguration(String serviceName, String name, Object config) {
        persistentCacheDescriptors.ensureHas(node.getSerializationService(), serviceName, name, config);
    }

    /**
     * Returns the distributed object name for the given {@link RamStoreRegistry} {@code prefix}.
     *
     * @param prefix the prefix of the {@link RamStoreRegistry}
     * @return the cache name
     * @throws IllegalArgumentException if there is no descriptor found for this prefix
     */
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
    public void memberAttributeChanged(MemberAttributeServiceEvent event) {
    }

    public void addClusterHotRestartEventListener(ClusterHotRestartEventListener listener) {
        this.clusterMetadataManager.addClusterHotRestartEventListener(listener);
    }

    public ClusterMetadataManager getClusterMetadataManager() {
        return clusterMetadataManager;
    }

    /**
     * Prepares the Hot restart store for {@link #start()}.
     * <ul>
     * <li>Checks for any mismatch of number of persisted stores and configured parallelism or persisted and current
     * partition thread count</li>
     * <li>Persists the partition thread count if there are no persisted stores</li>
     * <li>Prepares the cluster metadata by reading it from disk</li>
     * <li>Creates the hot restart off-heap and on-heap stores</li>
     * </ul>
     */
    public void prepare() {
        partitionThreadCount = getOperationExecutor().getPartitionThreadCount();
        int persistedStoreCount = persistedStoreCount();
        if (persistedStoreCount > 0) {
            if (storeCount != persistedStoreCount) {
                throw new HotRestartException(String.format(
                        "Mismatch between configured and actual level of parallelism in Hot Restart Persistence."
                                + " Configured %d, actual %d",
                        storeCount, persistedStoreCount));
            }
            int persistedPartitionThreadCount = clusterMetadataManager.readPartitionThreadCount();
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
        persistentCacheDescriptors.restore(node.getSerializationService(), loadedConfigurationListeners);
        clusterMetadataManager.prepare();
        createHotRestartStores();
    }

    /**
     * Starts the Hot restart service.
     * <ul>
     * <li>Starts the metadata manager which validates the cluster metadata loaded from disk</li>
     * <li>Restores the cache descriptors from disk</li>
     * <li>Loads the hot restart data from disk</li>
     * <li>Force starts if not all members joined and force start was requested</li>
     * </ul>
     *
     * @throws HotRestartException if there was any exception while starting the service
     */
    public void start() {
        try {
            logger.info("Starting hot-restart service. Base directory: " + hotRestartHome.getAbsolutePath());
            clusterMetadataManager.start();
            boolean allowData = clusterMetadataManager.isStartWithHotRestart();
            logger.info(allowData ? "Starting the Hot Restart procedure."
                    : "Initializing Hot Restart stores, not expecting to reload any data.");
            long start = currentTimeMillis();
            Throwable failure = null;
            try {
                runRestarterPipeline(onHeapStores, !allowData);
                runRestarterPipeline(offHeapStores, !allowData);
            } catch (ForceStartException e) {
                throw e;
            } catch (Throwable t) {
                failure = t;
            }
            clusterMetadataManager.loadCompletedLocal(failure);
            logger.info(String.format("Hot Restart procedure completed in %,d seconds",
                    MILLISECONDS.toSeconds(currentTimeMillis() - start)));
        } catch (ForceStartException e) {
            handleForceStart(true);
        } catch (HotRestartException e) {
            throw e;
        } catch (InterruptedException e) {
            currentThread().interrupt();
            throw new HotRestartException("Thread interrupted during the Hot Restart procedure", e);
        } catch (Throwable t) {
            throw new HotRestartException("Hot Restart procedure failed", t);
        }
    }

    /**
     * Creates a snapshot (backup) of the current state of the Hot Restart Store to a directory nested under the configured
     * {@link HotRestartPersistenceConfig#getBackupDir()} with the name backup-{@code sequence}. The backup will contain all
     * data managed by the Hot Restart service, including persistent cache descriptors, cluster metadata and partition data.
     * <p>
     * The completeness and consistency of the copied data is guaranteed if the cluster is in the {@link ClusterState#PASSIVE}
     * state but can be called in any cluster state. If being called while the cluster is in the {@link ClusterState#ACTIVE}
     * state, the user must make certain that no new persistent cache structures are being created or that there are no cluster
     * metadata changes, such as:
     * <ul>
     * <li>partition table data changes (replica changes)</li>
     * <li>cluster state changes</li>
     * <li>membership changes (including losing members)</li>
     * </ul>
     * In other cases the snapshot data can be inconsistent.
     * This method will return as soon as it has finished copying cache descriptor data, cluster metadata and started copying
     * partition data. Partition data will be copied asynchronously.
     *
     * @param sequence the backup sequence. This will determine the directory name for the backup
     * @return if the backup task was run
     */
    public boolean backup(long sequence) {
        if (hotRestartBackupDir == null) {
            logger.warning("Could not backup hot restart store, hot restart backup dir is not configured");
            return false;
        }
        if (isBackupInProgress()) {
            logger.fine("Backup is already in progress, aborting new backup");
            return false;
        }
        final File backupDir = new File(hotRestartBackupDir, BACKUP_DIR_PREFIX + sequence);
        ensureDir(backupDir);
        persistentCacheDescriptors.backup(backupDir);
        clusterMetadataManager.backup(backupDir);
        backup(backupDir, onHeapStores, true);
        backup(backupDir, offHeapStores, false);
        return true;
    }

    /** Returns true if there is a backup task currently in progress */
    public boolean isBackupInProgress() {
        return isBackupInProgress(onHeapStores) || isBackupInProgress(offHeapStores);
    }

    private static boolean isBackupInProgress(HotRestartStore[] stores) {
        if (stores != null) {
            for (HotRestartStore store : stores) {
                if (IN_PROGRESS.equals(store.getBackupTaskState())) {
                    return true;
                }
            }
        }
        return false;
    }

    private static void interruptBackupTask(HotRestartStore[] stores) {
        if (stores != null) {
            for (HotRestartStore store : stores) {
                if (IN_PROGRESS.equals(store.getBackupTaskState())) {
                    store.interruptBackupTask();
                }
            }
        }
    }

    private static BackupTaskStatus getBackupTaskStatus(HotRestartStore[] stores) {
        BackupTaskState state = null;
        int completed = 0;
        if (stores != null) {
            for (HotRestartStore store : stores) {
                final BackupTaskState storeBackupTaskState = store.getBackupTaskState();
                if (IN_PROGRESS.equals(storeBackupTaskState)) {
                    state = IN_PROGRESS;
                    continue;
                }
                if (FAILURE.equals(storeBackupTaskState) && state == null) {
                    state = FAILURE;
                }
                completed++;
            }
            return new BackupTaskStatus(state != null ? state : SUCCESS, completed, stores.length);
        }
        return new BackupTaskStatus(SUCCESS, 0, 0);
    }

    private void backup(File backupDir, HotRestartStore[] stores, boolean onHeap) {
        if (stores != null) {
            for (int i = 0; i < storeCount; i++) {
                final File storeDir = storeDir(i, onHeap);
                final File targetStoreDir = new File(backupDir, storeDir.getName());
                ensureDir(targetStoreDir);
                stores[i].backup(targetStoreDir);
            }
        }
    }

    private static void ensureDir(File dir) {
        try {
            final File canonicalDir = dir.getCanonicalFile();
            if (canonicalDir.exists()) {
                throw new HotRestartException("Path already exists : " + canonicalDir);
            }
            if (!canonicalDir.exists() && !canonicalDir.mkdirs()) {
                throw new HotRestartException("Could not create the directory " + canonicalDir);
            }
        } catch (IOException e) {
            throw new HotRestartException(e);
        }
    }

    public boolean isStartCompleted() {
        return clusterMetadataManager.isStartCompleted();
    }

    public boolean triggerForceStart() {
        InternalOperationService operationService = node.nodeEngine.getOperationService();
        Address masterAddress = node.getMasterAddress();
        if (node.isMaster()) {
            return clusterMetadataManager.handleForceStartRequest();
        } else if (masterAddress != null) {
            return operationService.send(new TriggerForceStartOnMasterOperation(), masterAddress);
        } else {
            logger.warning("Force start not triggered because there is no master member");
            return false;
        }
    }

    public boolean triggerPartialStart() {
        if (node.isMaster()) {
            return clusterMetadataManager.handlePartialStartRequest();
        } else {
            logger.warning("Partial data recovery not triggered because this node is not master");
            return false;
        }
    }

    public void shutdown() {
        clusterMetadataManager.shutdown();
        closeHotRestartStores();
        directoryLock.release();
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

    /**
     * Creates {@link HotRestartPersistenceConfig#getParallelism()} on-heap and off-heap hot restart stores. The off-heap
     * stores are created only if there is a defined memory manager.
     */
    private void createHotRestartStores() {
        HazelcastMemoryManager memMgr = ((EnterpriseNodeExtension) node.getNodeExtension()).getMemoryManager();

        HotRestartStore[] stores = new HotRestartStore[storeCount];
        for (int i = 0; i < storeCount; i++) {
            newHotRestartStoreConfig(i, true);
            stores[i] = newOnHeapHotRestartStore(newHotRestartStoreConfig(i, true), node.getProperties());
        }
        onHeapStores = stores;

        if (memMgr != null) {
            stores = new HotRestartStore[storeCount];
            for (int i = 0; i < storeCount; i++) {
                stores[i] = newOffHeapHotRestartStore(
                        newHotRestartStoreConfig(i, false).setMalloc(memMgr.getSystemAllocator()), node.getProperties());
            }
            offHeapStores = stores;
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
    private void runRestarterPipeline(HotRestartStore[] stores, final boolean failIfAnyData) throws Throwable {
        if (stores == null) {
            return;
        }
        assert stores.length == storeCount;
        final long deadline = cappedSum(currentTimeMillis(), dataLoadTimeoutMillis);
        final RamStoreRestartLoop loop = new RamStoreRestartLoop(stores.length, partitionThreadCount, this, logger);
        final AtomicReference<Throwable> failure = new AtomicReference<Throwable>();
        Thread[] restartThreads = new Thread[stores.length];
        for (int i = 0; i < stores.length; i++) {
            final int storeIndex = i;
            final HotRestartStore store = stores[storeIndex];
            restartThreads[i] = new Thread(store.name() + ".restart-thread") {
                @Override
                public void run() {
                    try {
                        store.hotRestart(failIfAnyData, storeCount, loop.keyReceivers[storeIndex],
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
            @Override
            public void run() {
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
                failure.compareAndSet(null, new HotRestartException("Timed out waiting for a restartThread to complete"));
            }
        }
        Throwable t = failure.get();
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

            HotRestartClusterStartStatus hotRestartStatus = clusterMetadataManager.getHotRestartStatus();
            Set<String> excludedMemberUuids = clusterMetadataManager.getExcludedMemberUuids();
            if (hotRestartStatus == CLUSTER_START_SUCCEEDED && excludedMemberUuids.contains(node.getThisUuid())) {
                throw new ForceStartException();
            }
        } while (!doneLatch.await(1, TimeUnit.SECONDS));
    }

    /**
     * resets local hot restart data and gets a new uuid, if the local node hasn't completed the start process and
     * it is excluded in cluster start.
     */
    public void resetHotRestartData() {
        if (isStartCompleted()) {
            throw new HotRestartException("cannot reset hot restart data since node has already started!");
        }

        final Set<String> excludedMemberUuids = clusterMetadataManager.getExcludedMemberUuids();
        if (!excludedMemberUuids.contains(node.getThisUuid())) {
            throw new HotRestartException("cannot reset hot restart data since this node is not excluded! excluded member uuids: "
                    + excludedMemberUuids);
        }

        handleForceStart(false);
    }

    /**
     * @param isAfterJoin if true, local node joins back to the cluster and completes the start process, after force-started.
     *                    Otherwise, it only resets itself, clears the hot restart data and gets a new uuid.
     */
    private void handleForceStart(boolean isAfterJoin) {
        if (!isAfterJoin && node.joined()) {
            logger.info("No need to reset hot restart data since node is joined and it will force-start itself.");
            return;
        }

        logger.warning("Force start requested, skipping hot restart");

        resetNode();

        resetHotRestart(isAfterJoin);

        logger.info("Resetting cluster state to ACTIVE");

        setClusterState(node.getClusterService(), ClusterState.ACTIVE, false);

        node.getJoiner().setTargetAddress(null);

        setNewLocalMemberUuid();

        if (isAfterJoin) {
            try {
                runRestarterPipeline(onHeapStores, true);
                runRestarterPipeline(offHeapStores, true);
            } catch (Throwable t) {
                throw new HotRestartException("starting hot restart threads after force start failed", t);
            }
        }

        // start connection-manager to setup and accept new connections
        node.connectionManager.start();

        if (isAfterJoin) {
            logger.info("Joining back...");

            // re-join to the target cluster
            node.join();

            clusterMetadataManager.forceStartCompleted();
        }
    }

    private void resetNode() {
        logger.info("Stopping connection manager...");
        node.connectionManager.stop();

        logger.info("Resetting node...");
        node.reset();

        logger.info("Resetting NodeEngine...");
        node.nodeEngine.reset();

        logger.fine("Resetting all services...");
        Collection<ManagedService> managedServices = node.nodeEngine.getServices(ManagedService.class);
        for (ManagedService service : managedServices) {
            if (service instanceof ClusterService) {
                continue;
            }
            service.reset();
        }
    }

    private void resetHotRestart(boolean isAfterJoin) {
        logger.info("Closing Hot Restart stores");
        closeHotRestartStores();

        logger.info("Deleting Hot Restart base-dir " + hotRestartHome);
        IOUtil.delete(hotRestartHome);

        logger.info("Resetting hot restart cluster metadata service...");
        clusterMetadataManager.reset();
        clusterMetadataManager.writePartitionThreadCount(getOperationExecutor().getPartitionThreadCount());
        persistentCacheDescriptors.reset();

        if (!isAfterJoin) {
            clusterMetadataManager.prepare();
        }

        logger.info("Creating thread local hot restart stores");
        createHotRestartStores();
    }

    private void setNewLocalMemberUuid() {
        node.setNewLocalMember();
        node.getClusterService().reset();
        OperationServiceImpl operationService = (OperationServiceImpl) node.nodeEngine.getOperationService();
        operationService.initInvocationContext();
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
        HotRestartStore[] stores = onHeapStores;
        onHeapStores = null;
        if (stores != null) {
            for (HotRestartStore st : stores) {
                st.close();
            }
        }

        stores = offHeapStores;
        offHeapStores = null;
        if (stores != null) {
            for (HotRestartStore st : stores) {
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

    /**
     * Interrupts the backup task if one is currently running. The contents of the target backup directory will be left as-is
     */
    public void interruptBackupTask() {
        interruptBackupTask(offHeapStores);
        interruptBackupTask(onHeapStores);
    }

    /**
     * Returns the local hot restart backup task status (not the cluster backup status).
     */
    public BackupTaskStatus getBackupTaskStatus() {
        final BackupTaskStatus offHeapStatus = getBackupTaskStatus(offHeapStores);
        final BackupTaskStatus onHeapStatus = getBackupTaskStatus(onHeapStores);

        final BackupTaskState state =
                IN_PROGRESS.equals(offHeapStatus.getState()) || IN_PROGRESS.equals(onHeapStatus.getState()) ? IN_PROGRESS
                        : FAILURE.equals(offHeapStatus.getState()) || FAILURE.equals(onHeapStatus.getState()) ? FAILURE
                        : SUCCESS;

        return new BackupTaskStatus(state,
                offHeapStatus.getCompleted() + onHeapStatus.getCompleted(),
                offHeapStatus.getTotal() + onHeapStatus.getTotal());
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

    private final class DirectoryLock {
        final FileChannel channel;
        final FileLock lock;

        private DirectoryLock() {
            File lockFile = new File(hotRestartHome, LOCK_FILE_NAME);
            channel = openChannel(lockFile);
            lock = acquireLock(lockFile);
        }

        private FileLock acquireLock(File lockFile) {
            FileLock fileLock = null;
            try {
                fileLock = channel.tryLock();
                if (fileLock == null) {
                    throw new HotRestartException("Cannot acquire lock on " + lockFile.getAbsolutePath()
                            + ". " + hotRestartHome + " is already being used by another member.");
                }
                return fileLock;
            } catch (OverlappingFileLockException e) {
                throw new HotRestartException("Cannot acquire lock on " + lockFile.getAbsolutePath()
                        + ". " + hotRestartHome + " is already being used by another member.", e);
            } catch (IOException e) {
                throw new HotRestartException("Unknown failure while acquiring lock on " + lockFile.getAbsolutePath(), e);
            } finally {
                if (fileLock == null) {
                    IOUtil.closeResource(channel);
                }
            }
        }

        private FileChannel openChannel(File lockFile) {
            try {
                return new RandomAccessFile(lockFile, "rw").getChannel();
            } catch (IOException e) {
                throw new HotRestartException("Cannot create lock file " + lockFile.getAbsolutePath(), e);
            }
        }

        void release() {
            try {
                lock.release();
                channel.close();
            } catch (IOException e) {
                logger.severe("Problem while releasing the lock and closing channel on "
                        + new File(hotRestartHome, LOCK_FILE_NAME), e);
            }
        }
    }
}
