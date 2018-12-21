package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.HotRestartClusterDataRecoveryPolicy;
import com.hazelcast.config.HotRestartPersistenceConfig;
import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.partition.PartitionTableView;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.hotrestart.ForceStartException;
import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.hotrestart.cluster.MemberClusterStartInfo.DataLoadStatus;
import com.hazelcast.spi.impl.executionservice.InternalExecutionService;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.util.Clock;
import com.hazelcast.version.Version;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import static com.hazelcast.config.HotRestartClusterDataRecoveryPolicy.FULL_RECOVERY_ONLY;
import static com.hazelcast.config.HotRestartClusterDataRecoveryPolicy.PARTIAL_RECOVERY_MOST_COMPLETE;
import static com.hazelcast.config.HotRestartClusterDataRecoveryPolicy.PARTIAL_RECOVERY_MOST_RECENT;
import static com.hazelcast.internal.cluster.impl.ClusterStateManagerAccessor.setClusterState;
import static com.hazelcast.internal.cluster.impl.ClusterStateManagerAccessor.setClusterVersion;
import static com.hazelcast.internal.cluster.impl.ClusterStateManagerAccessor.setMissingMembers;
import static com.hazelcast.spi.ExecutionService.SYSTEM_EXECUTOR;
import static com.hazelcast.spi.hotrestart.cluster.ClusterStateReader.readClusterState;
import static com.hazelcast.spi.hotrestart.cluster.ClusterVersionReader.readClusterVersion;
import static com.hazelcast.spi.hotrestart.cluster.HotRestartClusterStartStatus.CLUSTER_START_FAILED;
import static com.hazelcast.spi.hotrestart.cluster.HotRestartClusterStartStatus.CLUSTER_START_IN_PROGRESS;
import static com.hazelcast.spi.hotrestart.cluster.HotRestartClusterStartStatus.CLUSTER_START_SUCCEEDED;
import static com.hazelcast.spi.hotrestart.cluster.MemberClusterStartInfo.DataLoadStatus.LOAD_FAILED;
import static com.hazelcast.spi.hotrestart.cluster.MemberClusterStartInfo.DataLoadStatus.LOAD_IN_PROGRESS;
import static com.hazelcast.spi.hotrestart.cluster.MemberClusterStartInfo.DataLoadStatus.LOAD_SUCCESSFUL;
import static com.hazelcast.util.ThreadUtil.createThreadName;
import static java.lang.String.format;
import static java.lang.Thread.currentThread;
import static java.lang.Thread.sleep;
import static java.util.Collections.unmodifiableCollection;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * ClusterMetadataManager is responsible from loading cluster metadata
 * (cluster state, member list and partition table) during restart phase,
 * validating these metadata cluster-wide before restoring actual data
 * and storing these metadata when they change during runtime.
 */
@SuppressWarnings({"checkstyle:classdataabstractioncoupling", "checkstyle:methodcount", "checkstyle:classfanoutcomplexity"})
public class ClusterMetadataManager {

    private static final String DIR_NAME = "cluster";
    private static final long EXCLUDED_MEMBERS_LEAVE_WAIT_IN_MILLIS = TimeUnit.MINUTES.toMillis(2);

    private final Node node;
    private final File homeDir;
    private final ILogger logger;
    private final long validationTimeout;
    private final long dataLoadTimeout;
    private final AtomicReference<PartitionTableView> partitionTableRef = new AtomicReference<PartitionTableView>();
    private final HotRestartClusterDataRecoveryPolicy clusterDataRecoveryPolicy;
    private final ReentrantLock hotRestartStatusLock = new ReentrantLock();
    private final ConcurrentMap<Address, MemberClusterStartInfo> memberClusterStartInfos =
            new ConcurrentHashMap<Address, MemberClusterStartInfo>();
    private final AtomicReference<Collection<MemberImpl>> restoredMembersRef = new AtomicReference<Collection<MemberImpl>>();
    private final AtomicReference<Map<String, Address>> expectedMembersRef = new AtomicReference<Map<String, Address>>();
    private final List<ClusterHotRestartEventListener> hotRestartEventListeners =
            new CopyOnWriteArrayList<ClusterHotRestartEventListener>();
    private Thread pingThread;

    private volatile ClusterMetadataWriterLoop metadataWriterLoop;
    private volatile boolean startWithHotRestart = true;
    private volatile HotRestartClusterStartStatus hotRestartStatus = CLUSTER_START_IN_PROGRESS;
    private volatile boolean startCompleted;
    private volatile Set<String> excludedMemberUuids = Collections.emptySet();
    private volatile ClusterState clusterState = ClusterState.ACTIVE;
    private volatile long validationStartTime;
    private volatile long dataLoadStartTime;

    public ClusterMetadataManager(Node node, File hotRestartHome, HotRestartPersistenceConfig cfg) {
        this.node = node;
        logger = node.getLogger(getClass());
        homeDir = new File(hotRestartHome, DIR_NAME);
        validationTimeout = TimeUnit.SECONDS.toMillis(cfg.getValidationTimeoutSeconds());
        dataLoadTimeout = TimeUnit.SECONDS.toMillis(cfg.getDataLoadTimeoutSeconds());
        clusterDataRecoveryPolicy = cfg.getClusterDataRecoveryPolicy();
        metadataWriterLoop = new ClusterMetadataWriterLoop(homeDir, node);
    }

    /**
     * Prepares the cluster metadata manager for {@link #start()} by loading the existing data from disk.
     * <ul>
     * <li>Reads the cluster state</li>
     * <li>Restores the member list and prepares them for loading and validation</li>
     * <li>Restores the partition table and partition table version</li>
     * <li>If there is cluster state on disk sets the cluster state to {@code PASSIVE} and prepares the list of members removed
     * in not active state for validating member joins</li>
     * <li>Notifies any {@link ClusterHotRestartEventListener}s that the prepare is complete</li>
     * </ul>
     *
     * @throws HotRestartException if there was an {@link IOException} while preparing the metadata
     */
    // main thread
    public void prepare() {
        try {
            clusterState = readClusterState(node.getLogger(ClusterStateReader.class), homeDir);
            Version clusterVersion = readClusterVersion(node.getLogger(ClusterVersionReader.class), homeDir);
            if (!clusterVersion.isUnknown()) {
                // validate current codebase version is compatible with the persisted cluster version
                if (!node.getNodeExtension().isNodeVersionCompatibleWith(clusterVersion)) {
                    throw new HotRestartException("Member cannot start: codebase version " + node.getVersion() + " is not "
                            + "compatible with persisted cluster version " + clusterVersion);
                }
                setClusterVersion(node.clusterService, clusterVersion);
            }
            final Collection<MemberImpl> members = restoreMemberList();
            final PartitionTableView table = restorePartitionTable(members);
            if (startWithHotRestart) {
                ClusterServiceImpl clusterService = node.clusterService;
                setClusterState(clusterService, ClusterState.PASSIVE, true);
                List<MemberImpl> membersRemovedInNotActiveState = new ArrayList<MemberImpl>(members);
                membersRemovedInNotActiveState.remove(clusterService.getLocalMember());
                setMissingMembers(clusterService, membersRemovedInNotActiveState);
            }
            for (ClusterHotRestartEventListener listener : hotRestartEventListeners) {
                listener.onPrepareComplete(members, table, startWithHotRestart);
            }
        } catch (IOException e) {
            throw new HotRestartException(e);
        }
    }

    public int readPartitionThreadCount() {
        try {
            return PartitionThreadCountReader.readPartitionThreadCount(homeDir);
        } catch (IOException e) {
            throw new HotRestartException("Failed to read partition thread count from disk", e);
        }
    }

    public void writePartitionThreadCount(int count) {
        try {
            PartitionThreadCountWriter.writePartitionThreadCount(homeDir, count);
        } catch (IOException e) {
            throw new HotRestartException("Failed to write partition thread count = " + count + " to disk", e);
        }
    }

    public boolean isStartWithHotRestart() {
        return startWithHotRestart;
    }

    public void addClusterHotRestartEventListener(ClusterHotRestartEventListener listener) {
        this.hotRestartEventListeners.add(listener);
    }

    public Set<String> getExcludedMemberUuids() {
        // this is on purpose. We can start filling excludedMemberUuids before status is finalized
        // but we only publish it when the final status is set
        return hotRestartStatus == CLUSTER_START_SUCCEEDED ? excludedMemberUuids : Collections.<String>emptySet();
    }

    /**
     * Starts the metadata manager.
     * <ul>
     * <li>Awaits for all members to join if any metadata about members was loaded from disk</li>
     * <li>Validates the partition tables by sending them to the master</li>
     * <li>Sets the initial partition table and partition state version</li>
     * <li>Registers itself as a partition listener for persisting state on replica changes</li>
     * <li>Notifies all {@link ClusterHotRestartEventListener}s for data load start</li>
     * </ul>
     *
     * @throws ForceStartException if not all members joined but force start was requested
     * @throws HotRestartException if there were any other errors while starting
     */
    // main thread
    public void start() {
        metadataWriterLoop.start();
        try {
            validate();
        } catch (InterruptedException e) {
            currentThread().interrupt();
            throw new HotRestartException("Cluster metadata manager interrupted during startup");
        }
        setInitialPartitionTable();
        logger.info("Starting hot restart local data load.");
        for (ClusterHotRestartEventListener listener : hotRestartEventListeners) {
            listener.onDataLoadStart(node.getThisAddress());
        }
        dataLoadStartTime = Clock.currentTimeMillis();
        pingThread = new Thread(createThreadName(node.hazelcastInstance.getName(), "cluster-start-ping-thread")) {
            @Override
            public void run() {
                while (ping()) {
                    try {
                        logger.fine("Cluster start ping...");
                        sleep(SECONDS.toMillis(1));
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            }
        };
        pingThread.start();
    }

    private void validate() throws InterruptedException {
        validationStartTime = Clock.currentTimeMillis();
        if (completeValidationIfNoHotRestartData()) {
            return;
        }
        logger.info("Starting cluster member-list & partition table validation.");
        if (startWithHotRestart) {
            awaitUntilExpectedMembersJoin();
            logger.info("Expected members set after members join: " + expectedMembersRef.get());
            Map<Address, Address> addressMapping = getMemberAddressChangesMapping();
            logAddressChanges(addressMapping);
            repairRestoredMembers(addressMapping);
            repairPartitionTable(addressMapping);
        }
        callAfterExpectedMembersJoinListener();

        // THIS IS DONE HERE TO BE ABLE TO INVOKE LISTENERS
        Address thisAddress = node.getThisAddress();
        String thisUuid = node.getThisUuid();
        MemberClusterStartInfo clusterStartInfo = new MemberClusterStartInfo(partitionTableRef.get(), LOAD_IN_PROGRESS);
        receiveClusterStartInfoFromMember(thisAddress, thisUuid, clusterStartInfo);

        if (hotRestartStatus == CLUSTER_START_FAILED) {
            throw new HotRestartException("Cluster-wide start failed!");
        }
    }

    private void callAfterExpectedMembersJoinListener() {
        if (hotRestartEventListeners.isEmpty()) {
            return;
        }
        Collection<MemberImpl> members = new ArrayList<MemberImpl>();
        Map<String, Address> expectedMembers = expectedMembersRef.get();
        if (expectedMembers != null) {
            for (MemberImpl member : restoredMembersRef.get()) {
                if (expectedMembers.containsKey(member.getUuid())) {
                    members.add(member);
                }
            }
        }
        members = unmodifiableCollection(members);
        for (ClusterHotRestartEventListener listener : hotRestartEventListeners) {
            listener.afterExpectedMembersJoin(members);
        }
    }

    private boolean ping() {
        hotRestartStatusLock.lock();
        try {
            long deadline = dataLoadStartTime + dataLoadTimeout;
            if (hotRestartStatus != CLUSTER_START_IN_PROGRESS || deadline <= Clock.currentTimeMillis()) {
                logger.fine("Completing cluster start ping...");
                return false;
            }
            if (node.isMaster()) {
                askForClusterStartResult();
            } else {
                sendLocalMemberClusterStartInfoToMaster();
            }
            return true;
        } finally {
            hotRestartStatusLock.unlock();
        }
    }

    // main thread
    public void loadCompletedLocal(Throwable failure) throws InterruptedException {
        boolean success = failure == null;
        if (success) {
            logger.info("Local Hot Restart procedure completed with success.");
        } else {
            logger.warning("Local Hot Restart procedure completed with failure.", failure);
        }

        hotRestartStatusLock.lock();
        try {
            if (startWithHotRestart) {
                if (hotRestartStatus == CLUSTER_START_IN_PROGRESS) {
                    MemberClusterStartInfo current = memberClusterStartInfos.get(node.getThisAddress());
                    DataLoadStatus dataLoadStatus = success ? LOAD_SUCCESSFUL : LOAD_FAILED;
                    PartitionTableView partitionTable = current.getPartitionTable();
                    MemberClusterStartInfo newMemberClusterStartInfo = new MemberClusterStartInfo(partitionTable, dataLoadStatus);
                    receiveClusterStartInfoFromMember(node.getThisAddress(), node.getThisUuid(), newMemberClusterStartInfo);
                }
            } else {
                logger.info("Shortcutting cluster start to success as there is no hot restart data on disk.");
                hotRestartStatus = CLUSTER_START_SUCCEEDED;
            }
        } finally {
            hotRestartStatusLock.unlock();
        }

        try {
            waitForDataLoadTimeoutOrFinalClusterStartStatus();
            processFinalClusterStartStatus(failure);
            persistMembers();
            persistPartitions();
            restoredMembersRef.set(null);
            expectedMembersRef.set(null);
            partitionTableRef.set(null);
            memberClusterStartInfos.clear();
        } finally {
            try {
                pingThread.join();
            } catch (InterruptedException e) {
                logger.severe("Interrupted while joining to ping thread");
                currentThread().interrupt();
            }
        }
    }

    private void processFinalClusterStartStatus(Throwable failure) throws InterruptedException {
        if (hotRestartStatus == CLUSTER_START_SUCCEEDED) {
            if (excludedMemberUuids.contains(node.getThisUuid())) {
                invokeListenersOnCompletion();
                throw new ForceStartException();
            } else {
                awaitUntilExcludedMembersLeave();
                invokeListenersOnCompletion();
            }
        } else {
            invokeListenersOnCompletion();
            throw new HotRestartException("Cluster-wide start failed!", failure);
        }
    }

    private void invokeListenersOnCompletion() {
        for (ClusterHotRestartEventListener listener : hotRestartEventListeners) {
            listener.onHotRestartDataLoadComplete(hotRestartStatus, excludedMemberUuids);
        }
    }

    public void forceStartCompleted() {
        if (hotRestartStatus != CLUSTER_START_SUCCEEDED || excludedMemberUuids.contains(node.getThisUuid())) {
            throw new IllegalStateException("cannot complete force start with " + hotRestartStatus
                    + " and excluded member UUIDs: " + excludedMemberUuids);
        }
        startCompleted = true;
        logger.info("Force start completed.");
    }

    private void awaitUntilExcludedMembersLeave() throws InterruptedException {
        HotRestartClusterStartStatus hotRestartStatus = this.hotRestartStatus;
        if (hotRestartStatus != CLUSTER_START_SUCCEEDED) {
            throw new IllegalStateException("Cannot wait for excluded UUIDs to leave because in " + hotRestartStatus + " status");
        }

        long deadline = Clock.currentTimeMillis() + EXCLUDED_MEMBERS_LEAVE_WAIT_IN_MILLIS;
        while (isExcludedMemberPresentInCluster()) {
            sleep(SECONDS.toMillis(1));
            if (logger.isFineEnabled()) {
                long remaining = getRemainingDataLoadTimeMillis();
                logger.fine("Waiting for result... Remaining time: " + remaining + " ms.");
            }
            if (Clock.currentTimeMillis() > deadline) {
                throw new HotRestartException("Excluded members have not left the cluster before timeout!");
            }
        }

        hotRestartStatusLock.lock();
        try {
            setFinalClusterState(clusterState);
            startCompleted = true;
        } finally {
            hotRestartStatusLock.unlock();
        }
        notifyClusterServiceForExcludedMembers();
        logger.info("Completed hot restart with final cluster state: " + clusterState);
    }

    private boolean isExcludedMemberPresentInCluster() {
        for (String uuid : excludedMemberUuids) {
            if (node.getClusterService().getMember(uuid) != null) {
                return true;
            }
        }
        return false;
    }

    private void notifyClusterServiceForExcludedMembers() {
        for (Member member : restoredMembersRef.get()) {
            if (excludedMemberUuids.contains(member.getUuid())) {
                node.getClusterService().notifyForRemovedMember((MemberImpl) member);
            }
        }
    }

    public boolean isStartCompleted() {
        return startCompleted;
    }

    public void onMembershipChange() {
        final ClusterService clusterService = node.getClusterService();
        if (isStartCompleted() && clusterService.isJoined()) {
            persistMembers();
        } else if (hotRestartStatus == CLUSTER_START_IN_PROGRESS) {
            InternalExecutionService executionService = node.getNodeEngine().getExecutionService();
            executionService.execute(SYSTEM_EXECUTOR, new ClearMemberClusterStartInfoTask());
        }
    }

    public void onPartitionStateChange() {
        if (!node.getClusterService().isJoined()) {
            // Node is being shutdown.
            // Partition events at this point can be ignored,
            // latest partition state will be persisted during HotRestartIntegrationService shutdown.
            logger.finest("Skipping partition table change event, "
                    + "because node is shutting down and latest state will be persisted during shutdown.");
            return;
        }
        persistPartitions();
    }

    // operation thread
    public void onClusterStateChange(ClusterState newState) {
        if (logger.isFineEnabled()) {
            logger.fine("Will persist cluster state: " + newState);
        }
        metadataWriterLoop.writeClusterState(newState);
    }

    public HotRestartClusterStartStatus getHotRestartStatus() {
        return hotRestartStatus;
    }

    public void onClusterVersionChange(Version newClusterVersion) {
        if (logger.isFineEnabled()) {
            logger.fine("Will persist cluster version: " + newClusterVersion);
        }
        metadataWriterLoop.writeClusterVersion(newClusterVersion);

        // RU_COMPAT_3_11
        if (newClusterVersion.isEqualTo(Versions.V3_12)) {
            // persist partitions with the new format
            persistPartitions();
        }
    }

    File getHomeDir() {
        return homeDir;
    }

    public boolean handleForceStartRequest() {
        if (!node.isMaster()) {
            logger.warning("Force start attempt received but this node is not master!");
            return false;
        }

        hotRestartStatusLock.lock();
        try {
            if (hotRestartStatus != CLUSTER_START_IN_PROGRESS) {
                logger.warning("cannot trigger force start since cluster start status is " + hotRestartStatus);
                return false;
            }
            Set<String> excludedMemberUuids = new HashSet<String>();
            for (Member member : restoredMembersRef.get()) {
                excludedMemberUuids.add(member.getUuid());
            }
            this.excludedMemberUuids = unmodifiableSet(excludedMemberUuids);
            node.getClusterService().shrinkMissingMembers(this.excludedMemberUuids);
            this.clusterState = ClusterState.ACTIVE;
            this.hotRestartStatus = CLUSTER_START_SUCCEEDED;
            logger.warning("Force start is set.");
            broadcast(new SendClusterStartResultOperation(hotRestartStatus, excludedMemberUuids, clusterState));
            return true;
        } finally {
            hotRestartStatusLock.unlock();
        }
    }

    public boolean handlePartialStartRequest() {
        if (!node.isMaster()) {
            logger.warning("Partial data recovery request received but this node is not master!");
            return false;
        }
        if (!isPartialStartPolicy()) {
            logger.warning("Cannot trigger partial data recovery because cluster start policy is "
                    + clusterDataRecoveryPolicy);
            return false;
        }
        hotRestartStatusLock.lock();
        try {
            if (hotRestartStatus != CLUSTER_START_IN_PROGRESS) {
                logger.warning("Cannot trigger partial data recovery since cluster start status is " + hotRestartStatus);
                return false;
            }
            if (restoredMembersRef.get() == null) {
                logger.warning("Cannot trigger partial data recovery since restored member list is not present");
                return false;
            }
            if (!trySetCurrentMemberListToExpectedMembers()) {
                tryPartialStart();
            }
            return true;
        } finally {
            hotRestartStatusLock.unlock();
        }
    }

    private boolean trySetCurrentMemberListToExpectedMembers() {
        // early return if expectedMembersRef is already set
        if (expectedMembersRef.get() != null) {
            return false;
        }
        ClusterServiceImpl clusterService = node.getClusterService();
        Map<String, Address> expectedMembers = new HashMap<String, Address>();
        for (MemberImpl restoredMember : restoredMembersRef.get()) {
            MemberImpl currentMember = clusterService.getMember(restoredMember.getUuid());
            if (currentMember != null) {
                expectedMembers.put(currentMember.getUuid(), currentMember.getAddress());
            }
        }
        if (expectedMembersRef.compareAndSet(null, unmodifiableMap(expectedMembers))) {
            logger.info("Expected members are explicitly set to current members: " + expectedMembers);
            return true;
        }
        return false;
    }

    public void stopPersistence() {
        ClusterMetadataWriterLoop writerLoop = metadataWriterLoop;
        metadataWriterLoop = null;
        writerLoop.stop(false);
    }

    public void reset(boolean isAfterJoin) {
        metadataWriterLoop = new ClusterMetadataWriterLoop(homeDir, node);
        if (isAfterJoin) {
            metadataWriterLoop.start();
        }
        hotRestartStatusLock.lock();
        try {
            restoredMembersRef.set(null);
            expectedMembersRef.set(null);
            partitionTableRef.set(null);
            memberClusterStartInfos.clear();
        } finally {
            hotRestartStatusLock.unlock();
        }
    }

    public void shutdown() {
        if (isStartCompleted()) {
            persistMembers();
            logger.fine("Persisting partition table during shutdown");
            persistPartitions();
        }
        metadataWriterLoop.stop(true);
    }

    // operation thread
    void receiveClusterStartInfoFromMember(Address sender, String senderUuid, MemberClusterStartInfo senderClusterStartInfo) {
        if (!(node.isMaster() || node.getThisAddress().equals(sender))) {
            logger.warning("Ignoring partition table received from non-master " + sender + " since this node is not master!");
            return;
        }
        if (logger.isFineEnabled()) {
            logger.fine("Received partition table from " + sender);
        }
        hotRestartStatusLock.lock();
        try {
            if (hotRestartStatus == CLUSTER_START_FAILED) {
                logger.info("Cluster start already failed. Sending failure to: " + sender);
                sendIfNotThisMember(SendClusterStartResultOperation.newFailureResultOperation(), sender);
            } else if (hotRestartStatus == CLUSTER_START_SUCCEEDED) {
                validateSenderClusterStartInfoWhenSuccess(sender, senderUuid, senderClusterStartInfo);
            } else {
                validateSenderClusterStartInfoWhenInProgress(sender, senderClusterStartInfo);
            }
        } finally {
            hotRestartStatusLock.unlock();
        }
    }

    private void validateSenderClusterStartInfoWhenSuccess(Address sender, String senderUuid,
                                                           MemberClusterStartInfo senderClusterStartInfo) {
        if (excludedMemberUuids.contains(senderUuid)) {
            logger.info(sender + " with UUID: " + senderUuid + " is excluded in start.");
            Operation op = new SendClusterStartResultOperation(hotRestartStatus, excludedMemberUuids, null);
            sendIfNotThisMember(op, sender);
            return;
        } else if (excludedMemberUuids.contains(node.getThisUuid())) {
            logger.info("Will not answer " + sender + "'s cluster start info since this member is excluded in start.");
            return;
        }

        PartitionTableView senderPartitionTable = senderClusterStartInfo.getPartitionTable();
        DataLoadStatus senderDataLoadStatus = senderClusterStartInfo.getDataLoadStatus();
        PartitionTableView localPartitionTable;
        MemberClusterStartInfo thisMemberClusterStartInfo = memberClusterStartInfos.get(node.getThisAddress());
        if (thisMemberClusterStartInfo != null) {
            localPartitionTable = thisMemberClusterStartInfo.getPartitionTable();
        } else {
            localPartitionTable = node.partitionService.createPartitionTableView();
        }

        boolean partitionTableValidated = validatePartitionTable(localPartitionTable, senderPartitionTable);
        notifyListeners(sender, partitionTableValidated, senderDataLoadStatus);

        if (partitionTableValidated && senderDataLoadStatus == LOAD_SUCCESSFUL) {
            logger.info("Sender: " + sender + " succeeded after cluster is started");
            Operation op = new SendClusterStartResultOperation(CLUSTER_START_SUCCEEDED,
                    excludedMemberUuids, getCurrentClusterState());
            sendIfNotThisMember(op, sender);
        } else if (!partitionTableValidated || senderDataLoadStatus == LOAD_FAILED) {
            logger.info("Sender: " + sender + " failed after cluster is started. partition table validated: "
                    + partitionTableValidated + " sender data load result: " + senderDataLoadStatus);
            sendIfNotThisMember(SendClusterStartResultOperation.newFailureResultOperation(), sender);
        } else {
            logger.fine("Sender: " + sender + " validated its partition table but we are still waiting for data load result...");
        }
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
    private boolean validatePartitionTable(PartitionTableView localPartitionTable, PartitionTableView senderPartitionTable) {
        if (node.getClusterService().getClusterVersion().isGreaterOrEqual(Versions.V3_12)) {
            return localPartitionTable.equals(senderPartitionTable);
        }

        // RU_COMPAT_3_11
        if (localPartitionTable.getLength() != senderPartitionTable.getLength()) {
            return false;
        }
        if (localPartitionTable.getVersion() != senderPartitionTable.getVersion()) {
            return false;
        }
        for (int partition = 0; partition < localPartitionTable.getLength(); partition++) {
            for (int ix = 0; ix < InternalPartition.MAX_REPLICA_COUNT; ix++) {
                PartitionReplica localReplica = localPartitionTable.getReplica(partition, ix);
                PartitionReplica remoteReplica = senderPartitionTable.getReplica(partition, ix);
                if (localReplica == null) {
                    if (remoteReplica != null) {
                        return false;
                    }
                    continue;
                }
                if (!localReplica.address().equals(remoteReplica.address())) {
                    return false;
                }
            }
        }
        return true;
    }

    private void sendIfNotThisMember(Operation operation, Address target) {
        if (!node.getThisAddress().equals(target)) {
            InternalOperationService operationService = node.getNodeEngine().getOperationService();
            operationService.send(operation, target);
        }
    }

    private void validateSenderClusterStartInfoWhenInProgress(Address sender, MemberClusterStartInfo senderClusterStartInfo) {
        // MAY PUT STALE OBJECT TEMPORARILY BECAUSE OF NO ORDERING GUARANTEE
        // BUT EVENTUALLY THE FINAL OBJECT WILL BE PUT INSIDE
        memberClusterStartInfos.put(sender, senderClusterStartInfo);
        logger.fine("Received cluster info from member " + sender + " load-status " + senderClusterStartInfo.getDataLoadStatus());

        if (!memberClusterStartInfos.containsKey(node.getThisAddress())) {
            logger.fine("Not validating member cluster start info of " + sender
                    + " as this member's cluster start info is not known yet");
            return;
        }
        if (!validateMemberClusterStartInfosForFullStart(sender)) {
            autoTryPartialStart();
        }
        if (hotRestartStatus != CLUSTER_START_IN_PROGRESS) {
            ClusterState clusterState = hotRestartStatus == CLUSTER_START_SUCCEEDED ? this.clusterState : null;
            Operation op = new SendClusterStartResultOperation(hotRestartStatus, excludedMemberUuids, clusterState);
            sendIfNotThisMember(op, sender);
        }
    }

    private boolean validateMemberClusterStartInfosForFullStart(Address sender) {
        MemberClusterStartInfo thisMemberClusterStartInfo = memberClusterStartInfos.get(node.getThisAddress());
        int partitionTableVersion = thisMemberClusterStartInfo.getPartitionTableVersion();
        boolean fullStartSuccessful = true;

        for (Member member : restoredMembersRef.get()) {
            Address memberAddress = member.getAddress();
            MemberClusterStartInfo memberClusterStartInfo = memberClusterStartInfos.get(memberAddress);
            if (memberClusterStartInfo == null) {
                fullStartSuccessful = false;
                continue;
            }
            int memberPartitionTableVersion = memberClusterStartInfo.getPartitionTableVersion();
            boolean partitionTableValidated = validatePartitionTable(thisMemberClusterStartInfo.getPartitionTable(),
                    memberClusterStartInfo.getPartitionTable());
            DataLoadStatus memberDataLoadStatus = memberClusterStartInfo.getDataLoadStatus();

            if (memberAddress.equals(sender)) {
                notifyListeners(sender, partitionTableValidated, memberDataLoadStatus);
            }

            boolean memberFailed = !partitionTableValidated || memberDataLoadStatus == LOAD_FAILED;
            if (memberFailed) {
                fullStartSuccessful = false;
                if (clusterDataRecoveryPolicy == FULL_RECOVERY_ONLY) {
                    logger.warning("Failing cluster start since full cluster data recovery is expected and we have a failure! "
                            + "Failed member: " + memberAddress + ", reference partition table version: " + partitionTableVersion
                            + ", member partition table version: " + memberPartitionTableVersion
                            + " member load status: " + memberDataLoadStatus);
                    hotRestartStatus = CLUSTER_START_FAILED;
                    break;
                }
            } else if (memberDataLoadStatus == LOAD_IN_PROGRESS) {
                fullStartSuccessful = false;
            }
        }
        if (fullStartSuccessful) {
            logger.info("All members completed! Setting final result to success!");
            hotRestartStatus = CLUSTER_START_SUCCEEDED;
        }
        return fullStartSuccessful;
    }

    private void notifyListeners(Address sender, boolean isSenderPartitionTableValidated, DataLoadStatus memberDataLoadStatus) {
        for (ClusterHotRestartEventListener listener : hotRestartEventListeners) {
            listener.onPartitionTableValidationResult(sender, isSenderPartitionTableValidated);
        }
        if (memberDataLoadStatus != LOAD_IN_PROGRESS) {
            for (ClusterHotRestartEventListener listener : hotRestartEventListeners) {
                listener.onHotRestartDataLoadResult(sender, memberDataLoadStatus == LOAD_SUCCESSFUL);
            }
        }
    }

    private void autoTryPartialStart() {
        if (hotRestartStatus != CLUSTER_START_IN_PROGRESS) {
            logger.fine("No partial data recovery attempt since " + hotRestartStatus);
            return;
        }
        if (!isPartialStartPolicy()) {
            logger.fine("No partial data recovery attempt cluster start policy: " + clusterDataRecoveryPolicy);
            return;
        }
        if (checkPartialStart()) {
            return;
        }
        logger.fine("Auto partial data recovery attempt...");
        tryPartialStart();
    }

    private boolean checkPartialStart() {
        Map<String, Address> expectedMembers = expectedMembersRef.get();
        if (expectedMembers == null) {
            return true;
        }
        for (MemberImpl expectedMember : restoredMembersRef.get()) {
            if (!expectedMembers.containsKey(expectedMember.getUuid())) {
                continue;
            }
            if (!memberClusterStartInfos.containsKey(expectedMember.getAddress())) {
                return true;
            }
            MemberClusterStartInfo memberClusterStartInfo = memberClusterStartInfos.get(expectedMember.getAddress());
            if (memberClusterStartInfo.getDataLoadStatus() == LOAD_IN_PROGRESS) {
                logger.fine("No partial data recovery attempt since member load is in progress...");
                return true;
            }
        }
        return false;
    }

    private boolean isPartialStartPolicy() {
        return clusterDataRecoveryPolicy == PARTIAL_RECOVERY_MOST_COMPLETE
                || clusterDataRecoveryPolicy == PARTIAL_RECOVERY_MOST_RECENT;
    }

    public void receiveHotRestartStatus(Address sender, HotRestartClusterStartStatus result,
                                        Set<String> excludedMemberUuids, ClusterState clusterState) {
        if (node.getClusterService().isJoined()) {
            Address master = node.getMasterAddress();
            if (master.equals(sender) || node.isMaster()) {
                handleHotRestartStatus(sender, result, excludedMemberUuids, clusterState);
            } else {
                logger.warning(format("Received cluster start status from a non-master member %s. Current master is %s",
                        sender, master));
            }
        } else {
            handleHotRestartStatus(sender, result, excludedMemberUuids, clusterState);
        }
    }

    private void handleHotRestartStatus(Address sender, HotRestartClusterStartStatus result,
                                        Set<String> excludedMemberUuids, ClusterState clusterState) {
        if (!(result == CLUSTER_START_FAILED || result == CLUSTER_START_SUCCEEDED)) {
            throw new IllegalArgumentException("Cannot set hot restart status to " + result);
        }

        hotRestartStatusLock.lock();
        try {
            if (hotRestartStatus == CLUSTER_START_IN_PROGRESS) {
                logger.info("Setting cluster-wide start status to " + result + " with cluster state " + clusterState
                        +  " received from: " + sender);

                // ORDER IS IMPORTANT. excludedMemberUuids is set before hotRestartStatus since hotRestartStatus can be read
                // without acquiring the lock and excludedMemberUuids should be there once it is success.
                this.excludedMemberUuids = unmodifiableSet(new HashSet<String>(excludedMemberUuids));
                node.getClusterService().shrinkMissingMembers(this.excludedMemberUuids);
                if (result == CLUSTER_START_SUCCEEDED && !excludedMemberUuids.contains(node.getThisUuid())) {
                    this.clusterState = clusterState;
                }
                hotRestartStatus = result;
            } else if (hotRestartStatus != result) {
                logger.severe("Current cluster status: " + hotRestartStatus + " received cluster status: " + result
                        + " from: " + sender);
            }
        } finally {
            hotRestartStatusLock.unlock();
        }
    }

    // main & operation thread
    private void setFinalClusterState(ClusterState newState) {
        logger.info("Setting final cluster state to: " + newState);
        setClusterState(node.getClusterService(), newState, false);
    }

    private void setInitialPartitionTable() {
        PartitionTableView partitionTable = partitionTableRef.get();
        node.partitionService.setInitialState(partitionTable);
    }

    private PartitionTableView restorePartitionTable(Collection<MemberImpl> members) throws IOException {
        int partitionCount = node.getProperties().getInteger(GroupProperty.PARTITION_COUNT);
        PartitionTableView table;
        if (node.getClusterService().getClusterVersion().isGreaterOrEqual(Versions.V3_12)) {
            PartitionTableReader partitionTableReader = new PartitionTableReader(homeDir, partitionCount);
            partitionTableReader.read();
            table = partitionTableReader.getPartitionTable();
        } else {
            // RU_COMPAT_3_11
            LegacyPartitionTableReader partitionTableReader = new LegacyPartitionTableReader(homeDir, partitionCount);
            partitionTableReader.read();
            PartitionReplica[] replicas = PartitionReplica.from(members.toArray(new Member[0]));
            table = partitionTableReader.getPartitionTable(replicas);
        }

        partitionTableRef.set(table);
        if (logger.isFineEnabled()) {
            logger.fine("Restored partition table version " + table.getVersion());
        }
        return table;
    }

    private Collection<MemberImpl> restoreMemberList() throws IOException {
        MemberListReader r = new MemberListReader(homeDir);
        r.read();
        Member thisMember = r.getLocalMember();
        Collection<MemberImpl> members = r.getMembers();

        if (thisMember != null && !node.getThisAddress().equals(thisMember.getAddress())) {
            logger.warning("Local address change detected. Previous: " + thisMember.getAddress()
                    + ", Current: " + node.getThisAddress());
        }
        if (thisMember == null) {
            logger.info("Cluster state not found on disk. Will not load Hot Restart data.");
            startWithHotRestart = false;
            members = Collections.singletonList(node.getLocalMember());
        } else if (logger.isFineEnabled()) {
            logger.fine("Restored " + members.size() + " members -> " + members);
        }
        restoredMembersRef.set(members);
        return members;
    }

    private boolean completeValidationIfNoHotRestartData() {
        if (startWithHotRestart) {
            return false;
        }
        int memberListSize = restoredMembersRef.get().size();
        logger.info("No need to start validation since expected member count is: " + memberListSize);
        for (ClusterHotRestartEventListener listener : hotRestartEventListeners) {
            listener.onSingleMemberCluster();
        }
        return true;
    }

    private void logAddressChanges(Map<Address, Address> addressMapping) {
        if (!addressMapping.isEmpty()) {
            StringBuilder s = new StringBuilder("Address changes detected:");
            for (Map.Entry<Address, Address> entry : addressMapping.entrySet()) {
                s.append("\n\t").append(entry.getKey()).append(" -> ").append(entry.getValue());
            }
            logger.warning(s.toString());
        }
    }

    private void repairRestoredMembers(Map<Address, Address> addressMapping) {
        if (addressMapping.isEmpty()) {
            return;
        }
        Collection<MemberImpl> updatedMembers = new HashSet<MemberImpl>();
        for (MemberImpl member : restoredMembersRef.get()) {
            Address newAddress = addressMapping.get(member.getAddress());
            if (newAddress == null) {
                updatedMembers.add(member);
            } else {
                updatedMembers.add(new MemberImpl(newAddress, member.getVersion(), member.localMember(), member.getUuid()));
            }
        }
        restoredMembersRef.set(updatedMembers);
    }

    private void repairPartitionTable(Map<Address, Address> addressMapping) {
        if (addressMapping.isEmpty()) {
            return;
        }
        StringBuilder s = new StringBuilder("Replacing old addresses with the new ones in restored partition table:");
        for (Map.Entry<Address, Address> entry : addressMapping.entrySet()) {
            s.append("\n\t").append(entry.getKey()).append(" -> ").append(entry.getValue());
        }
        logger.info(s.toString());

        PartitionTableView table = partitionTableRef.get();
        PartitionReplica[][] newReplicas = new PartitionReplica[table.getLength()][];
        int versionInc = 0;
        for (int p = 0; p < newReplicas.length; p++) {
            PartitionReplica[] replicas = table.getReplicas(p);
            newReplicas[p] = replicas;

            for (int i = 0; i < InternalPartition.MAX_REPLICA_COUNT; i++) {
                PartitionReplica current = replicas[i];
                if (current == null) {
                    continue;
                }
                Address newAddress = addressMapping.get(current.address());
                if (newAddress == null) {
                    continue;
                }
                assert !current.address().equals(newAddress);
                replicas[i] = new PartitionReplica(newAddress, current.uuid());
                versionInc++;
            }
        }
        int version = table.getVersion() + versionInc;
        partitionTableRef.set(new PartitionTableView(newReplicas, version));
        logger.fine("Partition table repair has been completed. New partition table version: " + version);
    }

    /**
     * Awaits until all or a subset of restored members are joined.
     * <p>
     * If {@code clusterDataRecoveryPolicy} is {@code FULL_RECOVERY_ONLY) then expected members to join
     * will be all restored members. Otherwise, expected members are set to currently joined members
     * when validation timeout occurs or it's explicitly set by partial start trigger. Non-master members
     * periodically ask master for expected members to await.
     * <p>
     * When an unexpected member joins, a member that doesn't exist in restores members, await fails
     * with {@code HotRestartException}.
     *
     * @return returns a mapping of old and new addresses of joined members if an address change is detected
     */
    private void awaitUntilExpectedMembersJoin() throws InterruptedException {
        final Set<String> restoredMemberUuids = getRestoredMemberUuids();
        final ClusterServiceImpl clusterService = node.getClusterService();
        Map<String, Address> expectedMembers = new HashMap<String, Address>();

        while (expectedMembersRef.get() == null) {
            if (node.isMaster()) {
                expectedMembers.clear();
                if (isExpectedMembersJoined(expectedMembers)) {
                    trySetExpectedMembers(expectedMembers);
                    return;
                }
            } else {
                logger.info("Waiting for cluster formation... Expected-Size: " + restoredMemberUuids.size()
                        + ", Actual-Size: " + clusterService.getSize()
                        + ". Start-time: " + new Date(validationStartTime)
                        + ", Timeout: " + MILLISECONDS.toSeconds(validationTimeout) + " sec.");
                sendIfNotThisMember(new AskForExpectedMembersOperation(), node.getMasterAddress());
            }

            Collection<MemberImpl> members = clusterService.getMemberImpls();
            failIfUnexpectedMemberJoins(restoredMemberUuids, members);
            failOrSetExpectedMembersIfValidationTimedOut(members);

            for (ClusterHotRestartEventListener listener : hotRestartEventListeners) {
                listener.beforeAllMembersJoin(members);
            }

            sleep(SECONDS.toMillis(1));
        }
    }

    /**
     * Fails with an {@code HotRestartException} if a member is joined but that doesn't exist in restored members.
     *
     * @param restoredMemberUuids restored member UUID set
     * @param members current members
     */
    private void failIfUnexpectedMemberJoins(Set<String> restoredMemberUuids, Collection<MemberImpl> members) {
        for (MemberImpl member : members) {
            if (!restoredMemberUuids.contains(member.getUuid())) {
                throw new HotRestartException("Unexpected member is joined: " + member
                        + ". Restored members: " + restoredMembersRef.get());
            }
        }
    }

    private Set<String> getRestoredMemberUuids() {
        Set<String> restoredMemberUuids = new HashSet<String>();
        for (MemberImpl member : restoredMembersRef.get()) {
            restoredMemberUuids.add(member.getUuid());
        }
        return unmodifiableSet(restoredMemberUuids);
    }

    private void trySetExpectedMembers(Map<String, Address> expectedMembers) {
        if (!node.isMaster()) {
            return;
        }
        if (expectedMembersRef.compareAndSet(null, unmodifiableMap(expectedMembers))) {
            logger.info("Expected members are set to: " + expectedMembers);
            broadcast(new SendExpectedMembersOperation(expectedMembers));
        }
    }

    /**
     * Checks for validation timeout. When timeout occurs, fails with {@code HotRestartException}
     * if {@code clusterDataRecoveryPolicy} is {@code FULL_RECOVERY_ONLY). If not {@code FULL_RECOVERY_ONLY) then
     * tries to set expected members to currently joined member list.
     *
     * @param members
     */
    private void failOrSetExpectedMembersIfValidationTimedOut(Collection<MemberImpl> members) {
        if (hotRestartStatus == CLUSTER_START_SUCCEEDED && excludedMemberUuids.contains(node.getThisUuid())) {
            throw new ForceStartException();
        } else if (hotRestartStatus == CLUSTER_START_FAILED) {
            throw new HotRestartException("Cluster-wide start failed!");
        } else if (getRemainingValidationTimeMillis() == 0) {
            if (clusterDataRecoveryPolicy == FULL_RECOVERY_ONLY) {
                throw new HotRestartException(
                        "Expected members didn't join, validation phase timed-out!" + " Expected member-count: "
                                + restoredMembersRef.get().size() + ", Actual member-count: " + members.size()
                                + ". Start-time: " + new Date(validationStartTime) + ", Timeout: " + MILLISECONDS
                                .toSeconds(validationTimeout) + " sec.");
            }
            if (node.isMaster() && isPartialStartPolicy()) {
                if (trySetCurrentMemberListToExpectedMembers()) {
                    broadcast(new SendExpectedMembersOperation(expectedMembersRef.get()));
                }
            }
        }
    }

    private boolean isExpectedMembersJoined(Map<String, Address> expectedMembers) {
        ClusterServiceImpl clusterService = node.getClusterService();
        Collection<MemberImpl> restoredMembers = restoredMembersRef.get();

        for (Member restoredMember : restoredMembers) {
            Member currentMember = clusterService.getMember(restoredMember.getUuid());
            if (currentMember == null) {
                logger.info("Waiting for cluster formation... Expected-Size: " + restoredMembers.size()
                        + ", Actual-Size: " + clusterService.getSize() + ", Missing member: " + restoredMember
                        + ". Start-time: " + new Date(validationStartTime)
                        + ", Timeout: " + MILLISECONDS.toSeconds(validationTimeout) + " sec.");

                return false;
            }
            expectedMembers.put(currentMember.getUuid(), currentMember.getAddress());
        }
        return true;
    }

    private Map<Address, Address> getMemberAddressChangesMapping() {
        Map<String, Address> expectedMembers = expectedMembersRef.get();

        // once we repair member-list & partition table, we don't allow address change anymore
        MemberImpl localMember = node.getLocalMember();
        if (!localMember.getAddress().equals(expectedMembers.get(localMember.getUuid()))) {
            throw new HotRestartException("Expected members doesn't contain local member"
                    + " or local address has been changed after expected members are determined!"
                    + " Expected member address: " + expectedMembers.get(localMember.getUuid())
                    + ", Local member: " + localMember);
        }

        Map<Address, Address> addressMapping = new HashMap<Address, Address>();
        for (Member restoredMember : restoredMembersRef.get()) {
            Address expectedAddress = expectedMembers.get(restoredMember.getUuid());
            if (expectedAddress == null) {
                continue;
            }
            if (!expectedAddress.equals(restoredMember.getAddress())) {
                addressMapping.put(restoredMember.getAddress(), expectedAddress);
            }
        }
        return addressMapping;
    }

    private void sendLocalMemberClusterStartInfoToMaster() {
        Address masterAddress = node.getMasterAddress();
        if (masterAddress == null) {
            logger.warning("Failed to send partition table to master since master address is null");
            return;
        } else if (masterAddress.equals(node.getThisAddress())) {
            logger.warning("Failed to send partition table to master since this node is master.");
            return;
        }
        MemberClusterStartInfo memberClusterStartInfo = memberClusterStartInfos.get(node.getThisAddress());
        if (memberClusterStartInfo == null) {
            logger.fine("No member cluster start info to send to master!");
            return;
        }

        PartitionTableView partitionTable = memberClusterStartInfo.getPartitionTable();
        if (logger.isFinestEnabled()) {
            logger.finest("Sending partition table to: " + masterAddress + ", TABLE-> " + partitionTable);
        } else if (logger.isFineEnabled()) {
            logger.fine("Sending partition table to: " + masterAddress + ", Version: " + partitionTable.getVersion());
        }
        InternalOperationService operationService = node.getNodeEngine().getOperationService();
        operationService.send(new SendMemberClusterStartInfoOperation(memberClusterStartInfo), masterAddress);
    }

    // main thread
    private void waitForDataLoadTimeoutOrFinalClusterStartStatus()
            throws InterruptedException {
        Collection<HotRestartClusterStartStatus> expectedStatuses = EnumSet.of(CLUSTER_START_FAILED, CLUSTER_START_SUCCEEDED);
        while (!expectedStatuses.contains(hotRestartStatus)) {
            sleep(SECONDS.toMillis(1));
            if (logger.isFineEnabled()) {
                long remaining = getRemainingDataLoadTimeMillis();
                logger.fine("Waiting for result... Remaining time: " + remaining + " ms.");
            }
            failIfDataLoadDeadlineMissed();
        }
    }

    void receiveExpectedMembersFromMaster(Address sender, Map<String, Address> expectedMembers) {
        if (node.isMaster()) {
            logger.warning("Received expected members from " + sender + " but this node is already master.");
            return;
        } else if (!sender.equals(node.getMasterAddress())) {
            logger.warning("Received expected members from non-master member: " + sender
                    + ", current master is " + node.getMasterAddress() + ", expected member list is " + expectedMembers);
            return;
        }

        hotRestartStatusLock.lock();
        try {
            if (hotRestartStatus == CLUSTER_START_IN_PROGRESS) {
                Set<String> restoredMemberUuids = getRestoredMemberUuids();
                for (String expectedMemberUuid : expectedMembers.keySet()) {
                    if (!restoredMemberUuids.contains(expectedMemberUuid)) {
                        String message = "Invalid expected members are received from master: " + sender + ", "
                                + expectedMemberUuid + " doesn't exist in restored members: " + restoredMembersRef.get();
                        logger.severe(message);
                        return;
                    }
                }
                if (expectedMembersRef.compareAndSet(null, unmodifiableMap(expectedMembers))) {
                    logger.info("Expected members are set to " + expectedMembers + " received from master: " + sender);
                    return;
                }
                Map<String, Address> currentExpectedMembers = expectedMembersRef.get();
                if (!currentExpectedMembers.equals(expectedMembers)) {
                    logger.severe("Expected members are already set to " + currentExpectedMembers
                            + " but a different one " + expectedMembers + " is received from master: " + sender);
                }
            } else {
                logger.warning("Ignored expected members " + expectedMembers + " received from master: " + sender
                        + " because cluster start status is set to " + hotRestartStatus
                        + " with excluded members: " + excludedMemberUuids);
            }
        } finally {
            hotRestartStatusLock.unlock();
        }
    }

    void replyExpectedMembersQuestion(Address sender, String senderUuid) {
        if (!node.isMaster()) {
            logger.warning("Won't reply expected members question of sender: " + sender + " since this node is not master.");
            return;
        }

        hotRestartStatusLock.lock();
        try {
            if (hotRestartStatus == CLUSTER_START_FAILED
                    || (hotRestartStatus == CLUSTER_START_SUCCEEDED && excludedMemberUuids.contains(senderUuid))) {
                logger.info(sender + " with UUID: " + senderUuid + " is excluded in start.");
                Operation op = new SendClusterStartResultOperation(hotRestartStatus, excludedMemberUuids, null);
                sendIfNotThisMember(op, sender);
            } else if (hotRestartStatus == CLUSTER_START_IN_PROGRESS) {
                Map<String, Address> expectedMembers = expectedMembersRef.get();
                if (expectedMembers != null) {
                    sendIfNotThisMember(new SendExpectedMembersOperation(expectedMembers), sender);
                }
            } else {
                // cluster start is success. just send the current member list except the excluded ones
                ClusterServiceImpl clusterService = node.getClusterService();
                Map<String, Address> expectedMembers = new HashMap<String, Address>();
                for (Member member : clusterService.getActiveAndMissingMembers()) {
                    if (excludedMemberUuids.contains(member.getUuid())) {
                        continue;
                    }
                    expectedMembers.put(member.getUuid(), member.getAddress());
                }
                sendIfNotThisMember(new SendExpectedMembersOperation(expectedMembers), sender);
            }
        } finally {
            hotRestartStatusLock.unlock();
        }
    }

    private void failIfDataLoadDeadlineMissed() {
        if (getRemainingDataLoadTimeMillis() == 0) {
            if (!node.isMaster()) {
                if (clusterDataRecoveryPolicy != FULL_RECOVERY_ONLY) {
                    return;
                }
                throw new HotRestartException("Cluster-wide data load timeout...");
            }

            hotRestartStatusLock.lock();
            try {
                if (hotRestartStatus == CLUSTER_START_IN_PROGRESS) {
                    if (clusterDataRecoveryPolicy == FULL_RECOVERY_ONLY) {
                        logger.severe("Data load step timed out...");
                        hotRestartStatus = CLUSTER_START_FAILED;
                        broadcast(SendClusterStartResultOperation.newFailureResultOperation());
                    } else if (isPartialStartPolicy()) {
                        tryPartialStart();
                    } else {
                        throw new IllegalStateException("Invalid cluster start policy: " + clusterDataRecoveryPolicy);
                    }
                }
            } finally {
                hotRestartStatusLock.unlock();
            }
        }
    }

    private void tryPartialStart() {
        Map<Integer, List<String>> memberUuidsByPartitionTableVersion = collectLoadSucceededMemberUuidsByPartitionTableVersion();
        if (memberUuidsByPartitionTableVersion.isEmpty()) {
            logger.severe("Nobody has succeeded to load data...");
            hotRestartStatus = CLUSTER_START_FAILED;
            broadcast(SendClusterStartResultOperation.newFailureResultOperation());
        } else {
            Set<String> excludedMemberUuids = collectExcludedMemberUuids(memberUuidsByPartitionTableVersion);
            this.excludedMemberUuids = unmodifiableSet(excludedMemberUuids);
            node.getClusterService().shrinkMissingMembers(this.excludedMemberUuids);
            hotRestartStatus = CLUSTER_START_SUCCEEDED;
            logger.warning("Partial data recovery is set. Excluded member UUIDs: " + excludedMemberUuids);
            broadcast(new SendClusterStartResultOperation(hotRestartStatus, excludedMemberUuids, clusterState));
        }
    }

    private Map<Integer, List<String>> collectLoadSucceededMemberUuidsByPartitionTableVersion() {
        Map<Integer, List<String>> membersUuidsByPartitionTableVersion = new HashMap<Integer, List<String>>();
        final Map<String, Address> expectedMembers = expectedMembersRef.get();
        for (MemberImpl member : restoredMembersRef.get()) {
            if (!expectedMembers.containsKey(member.getUuid())) {
                continue;
            }
            MemberClusterStartInfo memberClusterStartInfo = memberClusterStartInfos.get(member.getAddress());
            if (memberClusterStartInfo == null || memberClusterStartInfo.getDataLoadStatus() != LOAD_SUCCESSFUL) {
                continue;
            }
            int partitionTableVersion = memberClusterStartInfo.getPartitionTableVersion();
            List<String> members = membersUuidsByPartitionTableVersion.get(partitionTableVersion);
            if (members == null) {
                members = new ArrayList<String>();
                membersUuidsByPartitionTableVersion.put(partitionTableVersion, members);
            }

            members.add(member.getUuid());
        }
        if (logger.isFineEnabled()) {
            logger.fine("Partition table version -> member UUIDs: " + membersUuidsByPartitionTableVersion);
        }
        return membersUuidsByPartitionTableVersion;
    }

    private Set<String> collectExcludedMemberUuids(Map<Integer, List<String>> membersByPartitionTableVersion) {
        int selectedPartitionTableVersion = -1;
        List<String> selectedMembers = Collections.emptyList();
        for (Map.Entry<Integer, List<String>> e : membersByPartitionTableVersion.entrySet()) {
            int partitionTableVersion = e.getKey();
            List<String> members = e.getValue();
            if (clusterDataRecoveryPolicy == PARTIAL_RECOVERY_MOST_RECENT) {
                if (partitionTableVersion > selectedPartitionTableVersion) {
                    selectedPartitionTableVersion = partitionTableVersion;
                    selectedMembers = members;
                }
            } else if (clusterDataRecoveryPolicy == PARTIAL_RECOVERY_MOST_COMPLETE) {
                if (members.size() > selectedMembers.size()
                        || (members.size() == selectedMembers.size() && partitionTableVersion > selectedPartitionTableVersion)) {
                    selectedPartitionTableVersion = partitionTableVersion;
                    selectedMembers = members;
                }
            }
            logger.fine("Candidate members " + selectedMembers
                    + " with partition table version: " + selectedPartitionTableVersion);
        }

        logger.info("Picking members " + selectedMembers
                + " with partition table version: " + selectedPartitionTableVersion);

        Set<String> excludedMemberUuids = new HashSet<String>();
        for (Member member : restoredMembersRef.get()) {
            if (!selectedMembers.contains(member.getUuid())) {
                excludedMemberUuids.add(member.getUuid());
            }
        }
        return excludedMemberUuids;
    }

    ClusterState getCurrentClusterState() {
        hotRestartStatusLock.lock();
        try {
            return startCompleted ? node.getClusterService().getClusterState() : clusterState;
        } finally {
            hotRestartStatusLock.unlock();
        }
    }

    private void askForClusterStartResult() {
        broadcast(new AskForClusterStartResultOperation());
    }

    private void persistMembers() {
        ClusterServiceImpl clusterService = node.getClusterService();
        Collection<Member> allMembers = clusterService.getActiveAndMissingMembers();
        if (logger.isFineEnabled()) {
            logger.fine("Will persist " + allMembers.size() + " (active & passive) members -> " + allMembers);
        }
        metadataWriterLoop.writeMembers(allMembers);
    }

    private void persistPartitions() {
        InternalPartitionService partitionService = node.getPartitionService();
        PartitionTableView partitionTable = partitionService.createPartitionTableView();
        if (partitionTable.getVersion() == 0) {
            if (logger.isFinestEnabled()) {
                logger.finest("Cannot persist partition table, not initialized yet.");
            }
            return;
        }
        if (logger.isFinestEnabled()) {
            logger.finest("Will persist partition table version: " + partitionTable.getVersion());
        }
        if (node.getClusterService().getClusterVersion().isGreaterOrEqual(Versions.V3_12)) {
            metadataWriterLoop.writePartitionTable(partitionTable);
        } else {
            // RU_COMPAT_3_11
            metadataWriterLoop.writePartitionTableLegacy(partitionTable);
        }
    }

    private void broadcast(Operation operation) {
        Map<String, Address> expectedMembers = expectedMembersRef.get();
        if (expectedMembers != null) {
            for (Address member : expectedMembers.values()) {
                sendIfNotThisMember(operation, member);
            }
            return;
        }
        for (Member member : restoredMembersRef.get()) {
            sendIfNotThisMember(operation, member.getAddress());
        }
    }

    /**
     * Copies the contents of the persisted cluster metadata to a folder with the name {@link #DIR_NAME} under the
     * {@code targetDir}. This method does not synchronize with the rest of the code so for consistent metadata it is
     * necessary to ensure that no cluster metadata changes are in progress during the duration of backup (e.g. no replica
     * or partition table changes).
     *
     * @param targetDir the directory under which the folder with the metadata will be copied
     */
    public void backup(File targetDir) {
        IOUtil.copy(homeDir, targetDir);
    }

    public String readMemberUuid() {
        LocalMemberReader r = new LocalMemberReader(homeDir);
        try {
            r.read();
        } catch (IOException e) {
            throw new HotRestartException("Cannot read local member UUID!", e);
        }
        Member localMember = r.getLocalMember();
        return localMember != null ? localMember.getUuid() : null;
    }

    HotRestartClusterDataRecoveryPolicy getClusterDataRecoveryPolicy() {
        return clusterDataRecoveryPolicy;
    }

    Collection<MemberImpl> getRestoredMembers() {
        Collection<MemberImpl> members = restoredMembersRef.get();
        if (members == null) {
            members = node.getClusterService().getMemberImpls();
        }
        return Collections.unmodifiableCollection(members);
    }

    MemberClusterStartInfo.DataLoadStatus getMemberDataLoadStatus(Address address) {
        MemberClusterStartInfo memberClusterStartInfo = memberClusterStartInfos.get(address);
        return memberClusterStartInfo != null ? memberClusterStartInfo.getDataLoadStatus() : null;
    }

    long getRemainingValidationTimeMillis() {
        if (validationStartTime == 0) {
            return validationTimeout;
        }
        long remaining = validationStartTime + validationTimeout - Clock.currentTimeMillis();
        return Math.max(0, remaining);
    }

    long getRemainingDataLoadTimeMillis() {
        if (dataLoadStartTime == 0) {
            return dataLoadTimeout;
        }
        long remaining = dataLoadStartTime + dataLoadTimeout - Clock.currentTimeMillis();
        return Math.max(0, remaining);
    }

    private class ClearMemberClusterStartInfoTask implements Runnable {
        @Override
        public void run() {
            hotRestartStatusLock.lock();
            try {
                if (hotRestartStatus == CLUSTER_START_IN_PROGRESS) {
                    Collection<MemberImpl> restoredMembers = restoredMembersRef.get();
                    if (restoredMembers == null) {
                        return;
                    }
                    ClusterServiceImpl clusterService = node.clusterService;
                    for (Member member : restoredMembers) {
                        Address address = member.getAddress();
                        if (clusterService.getMember(address) == null
                                && memberClusterStartInfos.remove(address) != null) {
                            logger.warning("Member cluster start info of " + address
                                    + " is removed as it has left the cluster");
                        }
                    }
                }
            } finally {
                hotRestartStatusLock.unlock();
            }
        }
    }
}
