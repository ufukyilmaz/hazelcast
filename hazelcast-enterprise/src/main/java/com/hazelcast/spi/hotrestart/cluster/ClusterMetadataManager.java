package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.HotRestartClusterDataRecoveryPolicy;
import com.hazelcast.config.HotRestartPersistenceConfig;
import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.PartitionTableView;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.hotrestart.ForceStartException;
import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.hotrestart.cluster.MemberClusterStartInfo.DataLoadStatus;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.util.Clock;
import com.hazelcast.version.ClusterVersion;

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
import static com.hazelcast.internal.cluster.impl.ClusterStateManagerAccessor.addMembersRemovedInNotActiveState;
import static com.hazelcast.internal.cluster.impl.ClusterStateManagerAccessor.setClusterState;
import static com.hazelcast.internal.cluster.impl.ClusterStateManagerAccessor.setClusterVersion;
import static com.hazelcast.spi.hotrestart.cluster.ClusterStateReader.readClusterState;
import static com.hazelcast.spi.hotrestart.cluster.ClusterVersionReader.readClusterVersion;
import static com.hazelcast.spi.hotrestart.cluster.HotRestartClusterStartStatus.CLUSTER_START_FAILED;
import static com.hazelcast.spi.hotrestart.cluster.HotRestartClusterStartStatus.CLUSTER_START_IN_PROGRESS;
import static com.hazelcast.spi.hotrestart.cluster.HotRestartClusterStartStatus.CLUSTER_START_SUCCEEDED;
import static com.hazelcast.spi.hotrestart.cluster.MemberClusterStartInfo.DataLoadStatus.LOAD_FAILED;
import static com.hazelcast.spi.hotrestart.cluster.MemberClusterStartInfo.DataLoadStatus.LOAD_IN_PROGRESS;
import static com.hazelcast.spi.hotrestart.cluster.MemberClusterStartInfo.DataLoadStatus.LOAD_SUCCESSFUL;
import static java.lang.String.format;
import static java.lang.Thread.currentThread;
import static java.lang.Thread.sleep;
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
public final class ClusterMetadataManager {

    private static final String DIR_NAME = "cluster";

    private static final long EXCLUDED_MEMBERS_LEAVE_WAIT_IN_MILLIS = TimeUnit.MINUTES.toMillis(2);

    private final Node node;
    private final MemberListWriter memberListWriter;
    private final PartitionTableWriter partitionTableWriter;
    private final ClusterStateWriter clusterStateWriter;
    private final ClusterVersionWriter clusterVersionWriter;
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
    private final AtomicReference<Set<String>> expectedMemberUuidsRef = new AtomicReference<Set<String>>();
    private final List<ClusterHotRestartEventListener> hotRestartEventListeners =
            new CopyOnWriteArrayList<ClusterHotRestartEventListener>();
    private Thread pingThread;

    private volatile boolean startWithHotRestart = true;
    private volatile HotRestartClusterStartStatus hotRestartStatus = CLUSTER_START_IN_PROGRESS;
    private volatile boolean startCompleted;
    private volatile Set<String> excludedMemberUuids = Collections.emptySet();
    private volatile ClusterState clusterState = ClusterState.ACTIVE;
    private volatile boolean addressChangeDetected;
    private volatile long validationStartTime;
    private volatile long dataLoadStartTime;

    public ClusterMetadataManager(Node node, File hotRestartHome, HotRestartPersistenceConfig cfg) {
        this.node = node;
        logger = node.getLogger(getClass());
        homeDir = new File(hotRestartHome, DIR_NAME);
        mkdirHome();
        validationTimeout = TimeUnit.SECONDS.toMillis(cfg.getValidationTimeoutSeconds());
        dataLoadTimeout = TimeUnit.SECONDS.toMillis(cfg.getDataLoadTimeoutSeconds());
        memberListWriter = new MemberListWriter(homeDir, node);
        clusterDataRecoveryPolicy = cfg.getClusterDataRecoveryPolicy();
        partitionTableWriter = new PartitionTableWriter(homeDir);
        clusterStateWriter = new ClusterStateWriter(homeDir);
        clusterVersionWriter = new ClusterVersionWriter(homeDir);
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
            ClusterVersion clusterVersion = readClusterVersion(node.getLogger(ClusterVersionReader.class), homeDir);
            if (clusterVersion != null) {
                // validate current codebase version is compatible with the persisted cluster version
                if (!node.getNodeExtension().isNodeVersionCompatibleWith(clusterVersion)) {
                    throw new HotRestartException("Member cannot start: codebase version " + node.getVersion() + " is not "
                            + "compatible with persisted cluster version " + clusterVersion);
                }
                setClusterVersion(node.clusterService, clusterVersion);
            }
            final Collection<MemberImpl> members = restoreMemberList();
            final PartitionTableView table = restorePartitionTable();
            if (startWithHotRestart) {
                ClusterServiceImpl clusterService = node.clusterService;
                setClusterState(clusterService, ClusterState.PASSIVE, true);
                addMembersRemovedInNotActiveState(clusterService, new ArrayList<MemberImpl>(members));
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
        pingThread = new Thread(node.getHazelcastThreadGroup().getThreadNamePrefix("cluster-start-ping-thread")) {
            @Override
            public void run() {
                while (ping()) {
                    try {
                        logger.fine("Cluster start ping...");
                        sleep(SECONDS.toMillis(1));
                    } catch (InterruptedException e) {
                        currentThread().interrupt();
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
            Map<Address, Address> addressMapping = awaitUntilAllMembersJoin();
            if (addressMapping.size() > 0) {
                addressChangeDetected = true;
            }
            logger.info("Expected member list after await until all members join: " + expectedMemberUuidsRef.get());
            logAddressChanges(addressMapping);
            fillMemberAddresses(addressMapping, restoredMembersRef);
            repairPartitionTable(addressMapping);
        }
        for (ClusterHotRestartEventListener listener : hotRestartEventListeners) {
            final Collection<MemberImpl> expectedMembers = new ArrayList<MemberImpl>();
            for (MemberImpl member : restoredMembersRef.get()) {
                if (expectedMemberUuidsRef.get().contains(member.getUuid())) {
                    expectedMembers.add(member);
                }
            }
            listener.afterAwaitUntilMembersJoin(expectedMembers);
        }

        // THIS IS DONE HERE TO BE ABLE TO INVOKE LISTENERS
        Address thisAddress = node.getThisAddress();
        String thisUuid = node.getThisUuid();
        MemberClusterStartInfo clusterStartInfo = new MemberClusterStartInfo(partitionTableRef.get(), LOAD_IN_PROGRESS);
        receiveClusterStartInfoFromMember(thisAddress, thisUuid, clusterStartInfo);

        if (hotRestartStatus == CLUSTER_START_FAILED) {
            throw new HotRestartException("Cluster-wide start failed!");
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
            expectedMemberUuidsRef.set(null);
            partitionTableRef.set(null);
            memberClusterStartInfos.clear();
        } finally {
            try {
                pingThread.join();
            } catch (InterruptedException e) {
                logger.severe("interrupted while joined to ping thread");
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
                    + " and excluded member uuids: " + excludedMemberUuids);
        }

        startCompleted = true;
        logger.info("Force start completed.");
    }

    private void awaitUntilExcludedMembersLeave() throws InterruptedException {
        HotRestartClusterStartStatus hotRestartStatus = this.hotRestartStatus;
        if (hotRestartStatus != CLUSTER_START_SUCCEEDED) {
            throw new IllegalStateException("Cannot wait for excluded uuids to leave because in " + hotRestartStatus + " status");
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
        if (isStartCompleted() && node.joined()) {
            persistMembers();
        } else if (hotRestartStatus == CLUSTER_START_IN_PROGRESS) {
            hotRestartStatusLock.lock();
            try {
                if (hotRestartStatus == CLUSTER_START_IN_PROGRESS) {
                    Collection<MemberImpl> restoredMembers = restoredMembersRef.get();
                    if (restoredMembers == null) {
                       return;
                    }

                    for (Member member : restoredMembers) {
                        Address address = member.getAddress();
                        if (node.getClusterService().getMember(address) == null
                                && memberClusterStartInfos.remove(address) != null) {
                            logger.warning("Member cluster start info of " + address + " is removed as it has left the cluster");
                        }
                    }
                }
            } finally {
                hotRestartStatusLock.unlock();
            }
        }
    }

    public void onPartitionStateChange() {
        if (!node.joined()) {
            // Node is being shutdown.
            // Partition events at this point can be ignored,
            // latest partition state will be persisted during HotRestartService shutdown.
            logger.finest("Skipping partition table change event, "
                    + "because node is shutting down and latest state will be persisted during shutdown.");
            return;
        }
        persistPartitions();
    }

    // operation thread
    public void onClusterStateChange(ClusterState newState) {
        if (logger.isFineEnabled()) {
            logger.fine("Persisting cluster state: " + newState);
        }
        try {
            clusterStateWriter.write(newState);
        } catch (IOException e) {
            logger.severe("While persisting cluster state: " + newState, e);
        }
    }

    public HotRestartClusterStartStatus getHotRestartStatus() {
        return hotRestartStatus;
    }

    public void onClusterVersionChange(ClusterVersion newClusterVersion) {
        if (logger.isFineEnabled()) {
            logger.fine("Persisting cluster version: " + newClusterVersion);
        }
        try {
            clusterVersionWriter.write(newClusterVersion);
        } catch (IOException e) {
            logger.severe("While persisting cluster state: " + newClusterVersion, e);
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
            node.getClusterService().shrinkMembersRemovedWhileClusterIsNotActiveState(this.excludedMemberUuids);
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
            logger.warning("Partial data recovery attempt received but this node is not master!");
            return false;
        }

        if (!isPartialStartPolicy()) {
            logger.warning("Cannot eagerly set expected member uuids because cluster start policy is "
                    + clusterDataRecoveryPolicy);
            return false;
        }

        hotRestartStatusLock.lock();
        try {
            if (hotRestartStatus != CLUSTER_START_IN_PROGRESS) {
                logger.warning("cannot trigger partial data recovery since cluster start status is " + hotRestartStatus);
                return false;
            }

            if (restoredMembersRef.get() == null) {
                logger.warning("cannot trigger partial data recovery since restored member list is not present");
                return false;
            }

            if (expectedMemberUuidsRef.get() == null) {
                setCurrentMemberListToExpectedMembers();
            } else {
                tryPartialStart();
            }

            return true;
        } finally {
            hotRestartStatusLock.unlock();
        }
    }

    private void setCurrentMemberListToExpectedMembers() {
        ClusterServiceImpl clusterService = node.getClusterService();
        Set<String> expectedMemberUuids = new HashSet<String>();
        for (MemberImpl restoredMember : restoredMembersRef.get()) {
            if (clusterService.getMember(restoredMember.getUuid()) != null) {
                expectedMemberUuids.add(restoredMember.getUuid());
            }
        }
        expectedMemberUuidsRef.set(unmodifiableSet(expectedMemberUuids));
        logger.info("Expected member list is eagerly set to current member list uuids: " + expectedMemberUuids);
    }

    public void reset() {
        hotRestartStatusLock.lock();
        try {
            restoredMembersRef.set(null);
            expectedMemberUuidsRef.set(null);
            partitionTableRef.set(null);
            memberClusterStartInfos.clear();
            mkdirHome();
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
            logger.info(sender + " with uuid: " + senderUuid + " is excluded in start.");
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

        boolean partitionTableValidated = localPartitionTable.equals(senderPartitionTable);

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
            boolean partitionTableValidated = thisMemberClusterStartInfo.checkPartitionTables(memberClusterStartInfo);
            DataLoadStatus memberDataLoadStatus = memberClusterStartInfo.getDataLoadStatus();

            if (memberAddress.equals(sender)) {
                notifyListeners(sender, partitionTableValidated, memberDataLoadStatus);
            }

            boolean memberFailed = !partitionTableValidated || memberDataLoadStatus == LOAD_FAILED;

            if (memberFailed) {
                fullStartSuccessful = false;
                if (clusterDataRecoveryPolicy == FULL_RECOVERY_ONLY) {
                    logger.warning("Failing cluster start since full cluster data recovery is expected and we have a failure! "
                            + "failed member: " + memberAddress + " ref partition table version: " + partitionTableVersion
                            + " ref partition table version: " + partitionTableVersion + " member partition table version: "
                            + memberPartitionTableVersion + " member load status: " + memberDataLoadStatus);
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

        logger.fine("auto partial data recovery attempt...");
        tryPartialStart();
    }

    private boolean checkPartialStart() {
        Set<String> expectedMemberUuids = expectedMemberUuidsRef.get();
        if (expectedMemberUuids == null) {
            return true;
        }

        for (MemberImpl expectedMember : restoredMembersRef.get()) {
            if (!expectedMemberUuids.contains(expectedMember.getUuid())) {
                continue;
            }

            if (!memberClusterStartInfos.containsKey(expectedMember.getAddress())) {
                return true;
            }

            MemberClusterStartInfo memberClusterStartInfo = memberClusterStartInfos.get(expectedMember.getAddress());
            if (memberClusterStartInfo.getDataLoadStatus() == LOAD_IN_PROGRESS) {
                logger.fine("No partial data recovery attempt since member load status...");
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
        if (node.joined()) {
            Address master = node.getMasterAddress();
            if (master.equals(sender) || node.isMaster()) {
                handleHotRestartStatus(sender, result, excludedMemberUuids, clusterState);
            } else {
                logger.warning(format(
                        "Received cluster start status from a non-master member %s. Current master is %s",
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
                node.getClusterService().shrinkMembersRemovedWhileClusterIsNotActiveState(this.excludedMemberUuids);
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

    private PartitionTableView restorePartitionTable() throws IOException {
        int partitionCount = node.getProperties().getInteger(GroupProperty.PARTITION_COUNT);
        PartitionTableReader partitionTableReader = new PartitionTableReader(homeDir, partitionCount);
        partitionTableReader.read();
        PartitionTableView table = partitionTableReader.getTable();
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
            addressChangeDetected = true;
            logger.warning("Local address change detected. Previous: " + thisMember.getAddress()
                    + ", Current: " + node.getThisAddress());
        }

        if (thisMember == null) {
            logger.info("Cluster state not found on disk. Will not load Hot Restart data.");
            startWithHotRestart = false;
            members = Collections.singletonList(node.getLocalMember());
        }
        if (logger.isFineEnabled()) {
            logger.fine("Restored " + members.size() + " members -> " + members);
        }
        restoredMembersRef.set(members);
        if (clusterDataRecoveryPolicy == FULL_RECOVERY_ONLY) {
            hotRestartStatusLock.lock();
            try {
                logger.fine("Setting expected member uuids with members -> " + members);
                final Set<String> uuids = new HashSet<String>();
                for (MemberImpl member : restoredMembersRef.get()) {
                    uuids.add(member.getUuid());
                }
                expectedMemberUuidsRef.set(unmodifiableSet(uuids));
            } finally {
                hotRestartStatusLock.unlock();
            }
        }

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

    private void fillMemberAddresses(Map<Address, Address> addressMapping, AtomicReference<Collection<MemberImpl>> ref) {
        if (!addressChangeDetected) {
            return;
        }

        Collection<MemberImpl> updatedMembers = new HashSet<MemberImpl>();
        for (MemberImpl member : ref.get()) {
            Address newAddress = addressMapping.get(member.getAddress());
            if (newAddress == null) {
                updatedMembers.add(member);
            } else {
                updatedMembers.add(new MemberImpl(newAddress, member.getVersion(), member.localMember(), member.getUuid(), null));
            }
        }

        ref.set(updatedMembers);
    }

    private void repairPartitionTable(Map<Address, Address> addressMapping) {
        if (!addressChangeDetected) {
            return;
        }

        assert !addressMapping.isEmpty() : "Address mapping should not be empty when address change is detected";

        StringBuilder s = new StringBuilder("Replacing old addresses with the new ones in restored partition table:");
        for (Map.Entry<Address, Address> entry : addressMapping.entrySet()) {
            s.append("\n\t").append(entry.getKey()).append(" -> ").append(entry.getValue());
        }
        logger.info(s.toString());

        PartitionTableView table = partitionTableRef.get();
        Address[][] newAddresses = new Address[table.getLength()][];

        int versionInc = 0;
        for (int p = 0; p < newAddresses.length; p++) {
            Address[] replicas = table.getAddresses(p);
            newAddresses[p] = replicas;

            for (int i = 0; i < InternalPartition.MAX_REPLICA_COUNT; i++) {
                Address current = replicas[i];
                if (current == null) {
                    continue;
                }
                Address updated = addressMapping.get(current);
                if (updated == null) {
                    continue;
                }
                assert !current.equals(updated);
                replicas[i] = updated;
                versionInc++;
            }
        }
        partitionTableRef.set(new PartitionTableView(newAddresses, table.getVersion() + versionInc));
        logger.fine("Partition table repair has been completed.");
    }

    private Map<Address, Address> awaitUntilAllMembersJoin() throws InterruptedException {
        Set<String> restoredMemberUuids = new HashSet<String>();
        for (MemberImpl member : restoredMembersRef.get()) {
            restoredMemberUuids.add(member.getUuid());
        }
        restoredMemberUuids = unmodifiableSet(restoredMemberUuids);

        Map<Address, Address> addressMapping = new HashMap<Address, Address>();

        final ClusterServiceImpl clusterService = node.getClusterService();
        while (true) {
            Collection<MemberImpl> members = clusterService.getMemberImpls();

            if (isExpectedMembersJoined(addressMapping)) {
                return addressMapping;
            }

            hotRestartStatusLock.lock();
            try {
                failIfAwaitUntilAllMembersJoinDeadlineMissed(members);

                for (ClusterHotRestartEventListener listener : hotRestartEventListeners) {
                    listener.beforeAllMembersJoin(members);
                }

                Address masterAddress = node.getMasterAddress();
                if (node.isMaster()) {
                    if (members.size() == restoredMemberUuids.size()
                            && expectedMemberUuidsRef.compareAndSet(null, restoredMemberUuids)) {
                        logger.info("All members are present. Expected member list is set to: " + restoredMemberUuids);
                        broadcast(new SendExpectedMemberUuidsOperation(expectedMemberUuidsRef.get()));
                    }
                } else if (masterAddress != null) {
                        sendIfNotThisMember(new AskForExpectedMembersOperation(), masterAddress);
                }
            } finally {
                hotRestartStatusLock.unlock();
            }

            sleep(SECONDS.toMillis(1));
        }
    }

    private void failIfAwaitUntilAllMembersJoinDeadlineMissed(Collection<MemberImpl> members) {
        if (getRemainingValidationTimeMillis() == 0) {
            if (clusterDataRecoveryPolicy == FULL_RECOVERY_ONLY) {
                throw new HotRestartException(
                        "Expected members didn't join, validation phase timed-out!"
                                + " Expected member-count: " + restoredMembersRef.get().size()
                                + ", Actual member-count: " + members.size()
                                + ". Start-time: " + new Date(validationStartTime)
                                + ", Timeout: " + MILLISECONDS.toSeconds(validationTimeout) + " sec.");
            } else if (node.isMaster() && isPartialStartPolicy() && expectedMemberUuidsRef.get() == null) {
                setCurrentMemberListToExpectedMembers();
                broadcast(new SendExpectedMemberUuidsOperation(expectedMemberUuidsRef.get()));
            }
        } else if (hotRestartStatus == CLUSTER_START_SUCCEEDED && excludedMemberUuids.contains(node.getThisUuid())) {
            throw new ForceStartException();
        } else if (hotRestartStatus == CLUSTER_START_FAILED) {
            throw new HotRestartException("Cluster-wide start failed!");
        }
    }

    private boolean isExpectedMembersJoined(Map<Address, Address> addressMapping) {
        addressMapping.clear();

        Set<String> expectedMemberUuids = expectedMemberUuidsRef.get();
        if (expectedMemberUuids == null) {
            return false;
        }

        ClusterServiceImpl clusterService = node.getClusterService();

        for (Member missingMember : restoredMembersRef.get()) {
            Member currentMember = clusterService.getMember(missingMember.getUuid());
            if (currentMember == null) {
                if (!expectedMemberUuids.contains(missingMember.getUuid())) {
                    continue;
                }

                logger.info("Waiting for cluster formation... Expected-Size: " + expectedMemberUuids.size()
                        + ", Actual-Size: " + clusterService.getSize()
                        + ", Missing member: " + missingMember);

                return false;
            }

            if (!currentMember.getAddress().equals(missingMember.getAddress())) {
                addressMapping.put(missingMember.getAddress(), currentMember.getAddress());
            }

        }

        return true;
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

    public void receiveExpectedMembersFromMaster(Address sender, Set<String> expectedMemberUuids) {
        if (node.isMaster()) {
            logger.warning("received expected member list from " + sender
                    + " but this node is already master.");
            return;
        } else if (!sender.equals(node.getMasterAddress())) {
            logger.warning("received expected member list from non-master member: " + sender
                    + " master is " + node.getMasterAddress() + " expected member list: " + expectedMemberUuids);
            return;
        }

        expectedMemberUuids = unmodifiableSet(new HashSet<String>(expectedMemberUuids));

        hotRestartStatusLock.lock();
        try {
            if (hotRestartStatus == CLUSTER_START_IN_PROGRESS) {
                if (expectedMemberUuidsRef.compareAndSet(null, expectedMemberUuids)) {
                    logger.info("expected member uuids is set to " + expectedMemberUuids + " received from master: " + sender);
                    return;
                }

                Set<String> currentExpectedMemberUuids = expectedMemberUuidsRef.get();
                if (!currentExpectedMemberUuids.equals(expectedMemberUuids)) {
                    logger.severe("expected member uuids is already set to " + currentExpectedMemberUuids
                            + " but a different one " + expectedMemberUuids + " is received from master: " + sender);
                }
            } else {
                logger.warning("Ignored expected member uuids " + expectedMemberUuids + " received from master: " + sender
                        + " because cluster start status is set to " + hotRestartStatus
                        + " with excluded members: " + excludedMemberUuids);
            }
        } finally {
            hotRestartStatusLock.unlock();
        }
    }

    public void replyExpectedMemberUuidsQuestion(Address sender, String senderUuid) {
        if (!node.isMaster()) {
            logger.warning("won't reply expected member list question of sender: " + sender + " since this node is not master.");
            return;
        }

        hotRestartStatusLock.lock();
        try {
            if (hotRestartStatus == CLUSTER_START_FAILED
                    || (hotRestartStatus == CLUSTER_START_SUCCEEDED && excludedMemberUuids.contains(senderUuid))) {
                logger.info(sender + " with uuid: " + senderUuid + " is excluded in start.");
                Operation op = new SendClusterStartResultOperation(hotRestartStatus, excludedMemberUuids, null);
                sendIfNotThisMember(op, sender);
            } else if (hotRestartStatus == CLUSTER_START_IN_PROGRESS) {
                Set<String> expectedMemberUuids = expectedMemberUuidsRef.get();
                if (expectedMemberUuids != null) {
                    sendIfNotThisMember(new SendExpectedMemberUuidsOperation(expectedMemberUuids), sender);
                }
            } else {
                // cluster start is success. just send the current member list except the excluded ones
                ClusterServiceImpl clusterService = node.getClusterService();
                Set<String> expectedMemberUuids = new HashSet<String>();
                for (Member member : clusterService.getCurrentMembersAndMembersRemovedWhileClusterIsNotActive()) {
                    if (excludedMemberUuids.contains(member.getUuid())) {
                        continue;
                    }

                    expectedMemberUuids.add(member.getUuid());
                }

                sendIfNotThisMember(new SendExpectedMemberUuidsOperation(expectedMemberUuids), sender);
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

                throw new HotRestartException("cluster-wide data load timeout...");
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
                        throw new IllegalStateException("invalid cluster start policy: " + clusterDataRecoveryPolicy);
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
            node.getClusterService().shrinkMembersRemovedWhileClusterIsNotActiveState(this.excludedMemberUuids);
            hotRestartStatus = CLUSTER_START_SUCCEEDED;

            logger.warning("Partial data recovery is set. Excluded member uuids: " + excludedMemberUuids);
            broadcast(new SendClusterStartResultOperation(hotRestartStatus, excludedMemberUuids, clusterState));
        }
    }

    private Map<Integer, List<String>> collectLoadSucceededMemberUuidsByPartitionTableVersion() {
        Map<Integer, List<String>> membersUuidsByPartitionTableVersion = new HashMap<Integer, List<String>>();
        final Set<String> expectedMemberUuids = expectedMemberUuidsRef.get();
        for (MemberImpl member : restoredMembersRef.get()) {
            if (!expectedMemberUuids.contains(member.getUuid())) {
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

        logger.info("partition table version -> member uuids: " + membersUuidsByPartitionTableVersion);

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
                    logger.info("Picking partition table version = " + selectedPartitionTableVersion
                            + " with members: " + selectedMembers);
                }
            } else if (clusterDataRecoveryPolicy == PARTIAL_RECOVERY_MOST_COMPLETE) {
                if (members.size() > selectedMembers.size()
                        || (members.size() == selectedMembers.size() && partitionTableVersion > selectedPartitionTableVersion)) {
                    selectedPartitionTableVersion = partitionTableVersion;
                    selectedMembers = members;
                    logger.info("Picking partition table version = " + selectedPartitionTableVersion
                            + " with members: " + selectedMembers);
                }
            }
        }
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
        try {
            ClusterServiceImpl clusterService = node.getClusterService();
            Collection<Member> allMembers = clusterService.getCurrentMembersAndMembersRemovedWhileClusterIsNotActive();
            if (logger.isFineEnabled()) {
                logger.fine("Persisting " + allMembers.size() + " (active & passive) members -> " + allMembers);
            }
            memberListWriter.write(allMembers);
        } catch (IOException e) {
            logger.severe("While persisting members", e);
        }
    }

    private void persistPartitions() {
        try {
            InternalPartitionService partitionService = node.getPartitionService();
            PartitionTableView partitionTable = partitionService.createPartitionTableView();
            if (partitionTable.getVersion() == 0) {
                if (logger.isFinestEnabled()) {
                    logger.finest("Cannot persist partition table, not initialized yet.");
                }
                return;
            }

            if (logger.isFinestEnabled()) {
                logger.finest("Persisting partition table version: " + partitionTable.getVersion());
            }
            partitionTableWriter.write(partitionTable);
        } catch (IOException e) {
            logger.severe("While persisting partition table", e);
        }
    }

    private void broadcast(Operation operation) {
        for (Member member : restoredMembersRef.get()) {
            sendIfNotThisMember(operation, member.getAddress());
        }
    }

    private void mkdirHome() {
        if (!homeDir.exists() && !homeDir.mkdirs()) {
            throw new HotRestartException("Cannot create Hot Restart base directory: " + homeDir.getAbsolutePath());
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
}
