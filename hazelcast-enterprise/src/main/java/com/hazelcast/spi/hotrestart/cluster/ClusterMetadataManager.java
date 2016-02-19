package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.config.HotRestartPersistenceConfig;
import com.hazelcast.core.Member;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.PartitionListener;
import com.hazelcast.internal.partition.impl.PartitionReplicaChangeEvent;
import com.hazelcast.spi.MembershipServiceEvent;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.hotrestart.ForceStartException;
import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.cluster.impl.ClusterStateManagerAccessor.addMembersRemovedInNotActiveState;
import static com.hazelcast.internal.cluster.impl.ClusterStateManagerAccessor.setClusterState;
import static com.hazelcast.spi.hotrestart.cluster.HotRestartClusterInitializationStatus.FORCE_STARTED;
import static com.hazelcast.spi.hotrestart.cluster.HotRestartClusterInitializationStatus.PARTITION_TABLE_VERIFIED;
import static com.hazelcast.spi.hotrestart.cluster.HotRestartClusterInitializationStatus.PENDING_VERIFICATION;
import static com.hazelcast.spi.hotrestart.cluster.HotRestartClusterInitializationStatus.VERIFICATION_AND_LOAD_SUCCEEDED;
import static com.hazelcast.spi.hotrestart.cluster.HotRestartClusterInitializationStatus.VERIFICATION_FAILED;

/**
 * ClusterMetadataManager is responsible from loading cluster metadata
 * (cluster state, member list and partition table) during restart phase,
 * validating these metadata cluster-wide before restoring actual data
 * and storing these metadata when they change during runtime.
 */
@SuppressWarnings({"checkstyle:classdataabstractioncoupling", "checkstyle:methodcount", "checkstyle:classfanoutcomplexity" })
public final class ClusterMetadataManager implements PartitionListener {

    private static final String DIR_NAME = "cluster";
    private static final int SLEEP_MILLIS = 1000;

    private final Node node;
    private final MemberListWriter memberListWriter;
    private final PartitionTableWriter partitionTableWriter;
    private final ClusterStateReaderWriter clusterStateReaderWriter;
    private final File homeDir;
    private final ILogger logger;
    private final long validationTimeout;
    private final long dataLoadTimeout;

    private volatile boolean startWithHotRestart = true;

    private final AtomicReference<HotRestartClusterInitializationStatus> hotRestartStatus =
            new AtomicReference<HotRestartClusterInitializationStatus>(PENDING_VERIFICATION);

    private long validationStartTime;
    private long loadStartTime;

    private final Set<Address> notValidatedAddresses = Collections.newSetFromMap(new ConcurrentHashMap<Address, Boolean>());

    private final Set<Address> notLoadedAddresses = Collections.newSetFromMap(new ConcurrentHashMap<Address, Boolean>());

    private final AtomicReference<Collection<Address>> memberListRef = new AtomicReference<Collection<Address>>();
    private final AtomicReference<Address[][]> partitionTableRef = new AtomicReference<Address[][]>();
    private final AtomicReference<Boolean> localLoadResult = new AtomicReference<Boolean>();
    private int partitionTableVersion;

    private final List<ClusterHotRestartEventListener> hotRestartEventListeners =
            new CopyOnWriteArrayList<ClusterHotRestartEventListener>();

    public ClusterMetadataManager(Node node, File hotRestartHome, HotRestartPersistenceConfig hotRestartPersistenceConfig) {
        this.node = node;
        logger = node.getLogger(getClass());
        homeDir = new File(hotRestartHome, DIR_NAME);
        mkdirHome();
        validationTimeout = TimeUnit.SECONDS.toMillis(hotRestartPersistenceConfig.getValidationTimeoutSeconds());
        dataLoadTimeout = TimeUnit.SECONDS.toMillis(hotRestartPersistenceConfig.getDataLoadTimeoutSeconds());
        memberListWriter = new MemberListWriter(homeDir, node.getThisAddress());
        partitionTableWriter = new PartitionTableWriter(homeDir);
        ILogger clusterStateReaderWriterLogger = node.getLogger(ClusterStateReaderWriter.class);
        clusterStateReaderWriter = new ClusterStateReaderWriter(clusterStateReaderWriterLogger, homeDir);
    }

    private void mkdirHome() {
        if (!homeDir.exists() && !homeDir.mkdirs()) {
            throw new HotRestartException("Cannot create " + homeDir.getAbsolutePath());
        }
    }

    public void addClusterHotRestartEventListener(final ClusterHotRestartEventListener listener) {
        this.hotRestartEventListeners.add(listener);
    }

    // main thread
    public void prepare() {
        try {
            Collection<Address> addresses = restoreMemberList();
            Address[][] table = restorePartitionTable();

            if (startWithHotRestart) {
                final ClusterServiceImpl clusterService = node.clusterService;
                setClusterState(clusterService, ClusterState.PASSIVE, false);
                addMembersRemovedInNotActiveState(clusterService, addresses);
            }

            for (ClusterHotRestartEventListener listener : hotRestartEventListeners) {
                listener.onPrepareComplete(addresses, table, startWithHotRestart);
            }
        } catch (IOException e) {
            throw new HotRestartException(e);
        }

        logger.info("Preparation completed...");
    }

    private Address[][] restorePartitionTable() throws IOException {
        int partitionCount = node.getGroupProperties().getInteger(GroupProperty.PARTITION_COUNT);
        PartitionTableReader partitionTableReader = new PartitionTableReader(homeDir, partitionCount);
        partitionTableReader.read();
        Address[][] table = partitionTableReader.getTable();
        partitionTableRef.set(table);
        partitionTableVersion = partitionTableReader.getPartitionVersion();
        return table;
    }

    private Collection<Address> restoreMemberList() throws IOException {
        MemberListReader memberListReader = new MemberListReader(homeDir);
        clusterStateReaderWriter.read();
        memberListReader.read();

        Address thisAddress = memberListReader.getThisAddress();
        Collection<Address> addresses = memberListReader.getAddresses();
        memberListRef.set(addresses);

        if (thisAddress == null && !addresses.isEmpty()) {
            throw new HotRestartException("Unexpected state! Could not load local member address from disk!");
        }

        if (thisAddress != null && !node.getThisAddress().equals(thisAddress)) {
            throw new HotRestartException("Wrong local address! Expected: "
                    + node.getThisAddress() + ", Actual: " + thisAddress);
        }

        if (thisAddress == null) {
            logger.info("Cluster state not found on disk. Will not load hot-restart data.");
            startWithHotRestart = false;
        }

        notValidatedAddresses.addAll(addresses);
        notLoadedAddresses.addAll(addresses);
        return addresses;
    }

    // main thread
    public void start() {
        validate();

        setInitialPartitionTable();

        node.partitionService.addPartitionListener(this);
        loadStartTime = Clock.currentTimeMillis();

        logger.info("Starting hot restart local data load.");

        for (ClusterHotRestartEventListener listener : hotRestartEventListeners) {
            listener.onDataLoadStart(node.getThisAddress());
        }
    }

    private void setInitialPartitionTable() {
        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) node.getPartitionService();
        partitionService.setInitialState(partitionTableRef.get(), partitionTableVersion);
    }

    public void shutdown() {
        if (node.getClusterService().getClusterState() == ClusterState.ACTIVE) {
            writeMembers();
            writePartitions();
        }
    }

    // main thread
    private void validate() {
        validationStartTime = Clock.currentTimeMillis();

        if (completeValidationIfSingleMember()) {
            return;
        }

        logger.info("Starting cluster member-list & partition table validation.");
        if (startWithHotRestart) {
            awaitUntilAllMembersJoin();
            logger.info("All expected members joined...");
        }

        for (ClusterHotRestartEventListener listener : hotRestartEventListeners) {
            listener.onAllMembersJoin(memberListRef.get());
        }

        notValidatedAddresses.remove(node.getThisAddress());

        EnumSet<HotRestartClusterInitializationStatus> statuses = EnumSet
                .of(VERIFICATION_FAILED, PARTITION_TABLE_VERIFIED);
        waitForFailureOrExpectedStatus(statuses, new ValidationTask(), validationStartTime + validationTimeout);

        final HotRestartClusterInitializationStatus status = hotRestartStatus.get();

        for (ClusterHotRestartEventListener listener : hotRestartEventListeners) {
            listener.onPartitionTableValidationComplete(status);
        }

        if (status == VERIFICATION_FAILED) {
            throw new HotRestartException("Cluster-wide validation failed!");
        }

        logger.info("Cluster member-list & partition table validation completed.");
    }

    private boolean completeValidationIfSingleMember() {
        final int memberListSize = memberListRef.get().size();
        if (memberListSize <= 1) {
            logger.info("No need to start validation since expected member count is: " + memberListSize);
            hotRestartStatus.set(PARTITION_TABLE_VERIFIED);
            for (ClusterHotRestartEventListener listener : hotRestartEventListeners) {
                listener.onSingleMemberCluster();
            }
            return true;
        }
        return false;
    }

    // main thread
    private void awaitUntilAllMembersJoin() {
        final Collection<Address> loadedAddresses = memberListRef.get();
        final ClusterServiceImpl clusterService = node.getClusterService();
        Set<Member> members = clusterService.getMembers();
        while (members.size() != loadedAddresses.size()) {

            if (validationStartTime + validationTimeout < Clock.currentTimeMillis()) {
                throw new HotRestartException(
                        "Expected number of members didn't join, validation phase timed-out!"
                                + " Expected member-count: " + loadedAddresses.size()
                                + ", Actual member-count: " + members.size()
                                + ". Start-time: " + new Date(validationStartTime)
                                + ", Timeout: " + TimeUnit.MILLISECONDS.toSeconds(validationTimeout) + " sec.");
            } else if (hotRestartStatus.get() == FORCE_STARTED) {
                throw new ForceStartException();
            }

            logger.info("Waiting for cluster formation... Expected: " + loadedAddresses.size() + ", Actual: " + members.size());

            Address masterAddress = node.getMasterAddress();
            if (masterAddress != null && !masterAddress.equals(node.getThisAddress())) {
                InternalOperationService operationService = node.getNodeEngine().getOperationService();
                operationService.send(new CheckIfMasterForceStartedOperation(), masterAddress);
            }

            sleep1s();
            members = clusterService.getMembers();
            for (ClusterHotRestartEventListener listener : hotRestartEventListeners) {
                listener.beforeAllMembersJoin(members);
            }
        }

        for (Address address : loadedAddresses) {
            if (clusterService.getMember(address) == null) {
                throw new HotRestartException("Member missing! " + address);
            }
        }
    }

    private void sleep1s() {
        try {
            Thread.sleep(SLEEP_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw ExceptionUtil.rethrow(e);
        }
    }

    // main thread
    private void sendPartitionTableToMaster() {
        Address masterAddress = node.getMasterAddress();
        if (masterAddress == null) {
            logger.warning("Failed to send partition table to master since master address is null.");
            return;
        } else if (masterAddress.equals(node.getThisAddress())) {
            logger.warning("Failed to send partition table to master since this node is master.");
            return;
        }

        Address[][] table = partitionTableRef.get();
        if (logger.isFinestEnabled()) {
            logger.finest("Sending partition table to: " + masterAddress + ", TABLE-> " + Arrays.deepToString(table));
        } else if (logger.isFineEnabled()) {
            logger.fine("Sending partition table to: " + masterAddress);
        }
        InternalOperationService operationService = node.getNodeEngine().getOperationService();
        Operation op = new SendPartitionTableForValidationOperation(table);
        operationService.send(op, masterAddress);
    }

    // operation thread
    void receivePartitionTableFromMember(Address sender, Address[][] remoteTable) {
        if (!node.isMaster()) {
            logger.warning("Ignoring partition table received from " + sender + " since this node is not master!");
            return;
        }

        if (logger.isFineEnabled()) {
            logger.fine("Received partition table from " + sender);
        }

        final HotRestartClusterInitializationStatus status = hotRestartStatus.get();
        if (status == VERIFICATION_FAILED) {
            logger.info("Partition table validation is already failed. Sending failure to: " + sender);
            InternalOperationService operationService = node.getNodeEngine().getOperationService();
            operationService.send(new SendPartitionTableValidationResultOperation(VERIFICATION_FAILED), sender);
        } else if (status == FORCE_STARTED) {
            logger.info("Ignored partition table from " + sender + " and sent force start response.");
            InternalOperationService operationService = node.getNodeEngine().getOperationService();
            operationService.send(new ForceStartMemberOperation(), sender);
        } else {
            validatePartitionTable(sender, remoteTable);
        }
    }

    // operation thread
    private void validatePartitionTable(Address sender, Address[][] remoteTable) {
        Address[][] localTable = partitionTableRef.get();
        if (localTable == null) {
            // this node is already running
            // sender node is doing a rolling-restart
            // gather local table from partition service
            localTable = createTableFromPartitionService();
        }
        boolean validated = Arrays.deepEquals(localTable, remoteTable);
        for (ClusterHotRestartEventListener listener : hotRestartEventListeners) {
            listener.onPartitionTableValidationResult(sender, validated);
        }

        if (validated) {
            processSuccessfulPartitionTableValidation(sender);
        } else {
            processFailedPartitionTableValidation(sender);
        }
    }

    private Address[][] createTableFromPartitionService() {
        InternalPartitionServiceImpl partitionService = node.partitionService;
        Address[][] table = new Address[partitionService.getPartitionCount()][InternalPartition.MAX_REPLICA_COUNT];
        for (InternalPartition partition : partitionService.getPartitions()) {
            int partitionId = partition.getPartitionId();
            for (int replica = 0; replica < InternalPartition.MAX_REPLICA_COUNT; replica++) {
                table[partitionId][replica] = partition.getReplicaAddress(replica);
            }
        }
        return table;
    }

    private void processSuccessfulPartitionTableValidation(Address sender) {
        InternalOperationService operationService = node.getNodeEngine().getOperationService();

        notValidatedAddresses.remove(sender);
        if (logger.isFineEnabled()) {
            logger.fine("Partition table validation successful for: " + sender + " not-validated: " + notValidatedAddresses);
        }

        if (notValidatedAddresses.isEmpty()) {
            hotRestartStatus.compareAndSet(PENDING_VERIFICATION, PARTITION_TABLE_VERIFIED);

            HotRestartClusterInitializationStatus result = hotRestartStatus.get();
            if (result == VERIFICATION_AND_LOAD_SUCCEEDED) {
                result = PARTITION_TABLE_VERIFIED;
                logger.info("Will send " + PARTITION_TABLE_VERIFIED + " instead of " + VERIFICATION_AND_LOAD_SUCCEEDED
                            + " to member: " + sender);
            } else if (logger.isFineEnabled()) {
                logger.fine("Partition table validation completed for all members. Sending " + result + " to: " + sender);
            }

            final Operation op = result == FORCE_STARTED ? new ForceStartMemberOperation()
                    : new SendPartitionTableValidationResultOperation(result);
            operationService.send(op, sender);
        }
    }

    private void processFailedPartitionTableValidation(Address sender) {
        InternalOperationService operationService = node.getNodeEngine().getOperationService();

        final HotRestartClusterInitializationStatus status = hotRestartStatus.get();
        if (status == VERIFICATION_AND_LOAD_SUCCEEDED) {
            logger.warning("Wrong partition table received from " + sender + " after load successfully completed cluster-wide");
            operationService.send(new SendPartitionTableValidationResultOperation(VERIFICATION_FAILED), sender);
        } else if (status == FORCE_STARTED) {
            logger.info("Wrong partition table received from " + sender + " after status is set to " + FORCE_STARTED);
            operationService.send(new ForceStartMemberOperation(), sender);
        } else {
            // we can only CAS if it is not already completed with the 2 status values above
            hotRestartStatus.compareAndSet(status, VERIFICATION_FAILED);
            final HotRestartClusterInitializationStatus result = hotRestartStatus.get();
            logger.info("Partition table validation failed for: " + sender + ", Current validation " + result);

            final Operation op = result == FORCE_STARTED ? new ForceStartMemberOperation()
                    : new SendPartitionTableValidationResultOperation(result);
            operationService.send(op, sender);

            // must be removed after the CAS operation above
            notValidatedAddresses.remove(sender);
        }
    }

    // operation thread
    void receiveHotRestartStatusFromMasterAfterPartitionTableVerification(Address sender,
                                                                          HotRestartClusterInitializationStatus result) {
        Address master = node.getMasterAddress();
        if (!master.equals(sender)) {
            logger.warning("Received partition table validation result from non-master member: " + sender + " master: "
                    + master);
            return;
        }

        if (!(result == VERIFICATION_FAILED || result == PARTITION_TABLE_VERIFIED)) {
            throw new IllegalArgumentException("Can not set hot restart status after partition table verification to " + result);
        }

        if (logger.isFineEnabled()) {
            logger.fine("Setting cluster-wide validation status " + hotRestartStatus.get() + " to " + result);
        }

        hotRestartStatus.compareAndSet(PENDING_VERIFICATION, result);
    }

    // main thread
    private void waitForFailureOrExpectedStatus(Collection<HotRestartClusterInitializationStatus> expectedStatusses,
                                                TimeoutableRunnable runnable, long deadLine) {
        while (!expectedStatusses.contains(hotRestartStatus.get())) {
            if (deadLine <= Clock.currentTimeMillis()) {
                runnable.onTimeout();
            } else if (hotRestartStatus.get() == FORCE_STARTED) {
                throw new ForceStartException();
            }

            try {
                runnable.run();
            } catch (Exception e) {
                throw new HotRestartException(e);
            }

            sleep1s();

            if (logger.isFineEnabled()) {
                logger.fine("Waiting for result... Remaining time: " + (deadLine - Clock.currentTimeMillis()) + " ms.");
            }
        }
    }

    // main thread
    public void loadCompletedLocal(final Throwable failure) {
        final boolean success = failure == null;
        logger.info("Local hot-restart load completed: " + success + ". Waiting for all members to load...");

        localLoadResult.set(success);
        receiveLoadCompletionStatusFromMember(node.getThisAddress(), success);

        EnumSet<HotRestartClusterInitializationStatus> statuses = EnumSet
                .of(VERIFICATION_FAILED, VERIFICATION_AND_LOAD_SUCCEEDED);

        waitForFailureOrExpectedStatus(statuses, new LoadTask(success), loadStartTime + dataLoadTimeout);

        final HotRestartClusterInitializationStatus status = hotRestartStatus.get();
        for (ClusterHotRestartEventListener listener : hotRestartEventListeners) {
            listener.onHotRestartDataLoadComplete(status);
        }

        if (status == VERIFICATION_FAILED) {
            throw new HotRestartException("Cluster-wide load failed!", failure);
        }

        logger.info("Cluster-wide load completed... ClusterState: " + node.getClusterService().getClusterState());

        writeMembers();
        writePartitions();
        memberListRef.set(null);
        partitionTableRef.set(null);
        localLoadResult.set(null);
    }

    public void reset() {
        memberListRef.set(null);
        partitionTableRef.set(null);
        localLoadResult.set(null);
        notLoadedAddresses.clear();
        notValidatedAddresses.clear();

        mkdirHome();
    }

    // main thread
    private void sendLoadCompleteToMaster(boolean success) {
        Address masterAddress = node.getMasterAddress();

        if (masterAddress == null) {
            logger.warning("Failed to send load-completion status [" + success + "] to master since master address is null.");
            return;
        } else if (masterAddress.equals(node.getThisAddress())) {
            logger.warning("Failed to send load-completion status [" + success + "] to master since this node is master.");
            return;
        }

        Address[][] table = partitionTableRef.get();

        if (logger.isFineEnabled()) {
            logger.fine("Sending load-completion status [" + success + "] to: " + masterAddress);
        } else if (logger.isFinestEnabled()) {
            logger.fine("Sending load-completion status [" + success + "] to: " + masterAddress + ", TABLE: "
                    + Arrays.deepToString(table));
        }

        InternalOperationService operationService = node.getNodeEngine().getOperationService();
        Operation op = new SendLoadCompletionForValidationOperation(table, success);
        operationService.send(op, masterAddress);
    }

    // operation thread
    void receiveLoadCompletionStatusFromMember(Address sender, boolean success) {
        if (logger.isFineEnabled()) {
            logger.fine("Received load-completion status [" + success + "] from: " + sender + " waiting for: "
                    + notLoadedAddresses);
        }

        for (ClusterHotRestartEventListener listener : hotRestartEventListeners) {
            listener.onHotRestartDataLoadResult(sender, success);
        }

        if (!success) {
            if (!node.isMaster()) {
                if (!node.getThisAddress().equals(sender)) {
                    logger.warning("Received load-completion status [false] from: " + sender + " but this is not master!");
                }
                return;
            }
            processFailedLoadCompletionStatus(sender);
        } else if (hotRestartStatus.get() == PARTITION_TABLE_VERIFIED) {
            processSuccessfulLoadCompletionStatusWhenPartitionTableVerified(sender);
        } else {
            sendClusterWideLoadCompletionResultIfAvailable(sender);
        }
    }

    private void sendClusterWideLoadCompletionResultIfAvailable(Address sender) {
        InternalOperationService operationService = node.getNodeEngine().getOperationService();
        final HotRestartClusterInitializationStatus status = hotRestartStatus.get();

        if (status == VERIFICATION_AND_LOAD_SUCCEEDED || status == VERIFICATION_FAILED) {
            if (!node.getThisAddress().equals(sender)) {
                final ClusterState clusterState = node.getClusterService().getClusterState();
                logger.info("Sending cluster-wide load-completion result " + status + " and cluster state: "
                            + clusterState +  " to: " + sender);
                operationService.send(new SendLoadCompletionStatusOperation(status, clusterState), sender);
            }
        } else if (status == FORCE_STARTED) {
            if (!node.getThisAddress().equals(sender)) {
                logger.info("Sending " + status + " to: " + sender);
                operationService.send(new ForceStartMemberOperation(), sender);
            }
        }
    }

    private void processFailedLoadCompletionStatus(Address sender) {
        InternalOperationService operationService = node.getNodeEngine().getOperationService();
        if (hotRestartStatus.compareAndSet(PARTITION_TABLE_VERIFIED, VERIFICATION_FAILED)) {
            for (ClusterHotRestartEventListener listener : hotRestartEventListeners) {
                listener.onHotRestartDataLoadComplete(VERIFICATION_FAILED);
            }
        }

        if (!node.getThisAddress().equals(sender)) {
            final HotRestartClusterInitializationStatus result = hotRestartStatus.get();

            if (result == FORCE_STARTED) {
                logger.info("Failed load status received from " + sender + " after status: " + FORCE_STARTED);
            } else if (result == VERIFICATION_AND_LOAD_SUCCEEDED) {
                logger.warning("Failed load status received from " + sender + " after load successfully completed cluster-wide");
            } else if (logger.isFineEnabled()) {
                logger.fine("Sending failure result to: " + sender + ", Current hot restart status: " + result);
            }

            final Operation op = result == FORCE_STARTED ? new ForceStartMemberOperation()
                    : new SendLoadCompletionStatusOperation(VERIFICATION_FAILED, ClusterState.PASSIVE);
            operationService.send(op, sender);
        }

        notLoadedAddresses.remove(sender);
    }

    private void processSuccessfulLoadCompletionStatusWhenPartitionTableVerified(Address sender) {
        notLoadedAddresses.remove(sender);

        if (notLoadedAddresses.isEmpty()) {
            if (hotRestartStatus.compareAndSet(PARTITION_TABLE_VERIFIED, VERIFICATION_AND_LOAD_SUCCEEDED)) {
                logger.info("Cluster wide hot restart status is set to " + VERIFICATION_AND_LOAD_SUCCEEDED);
                for (ClusterHotRestartEventListener listener : hotRestartEventListeners) {
                    listener.onHotRestartDataLoadComplete(VERIFICATION_AND_LOAD_SUCCEEDED);
                }

                setFinalClusterState(clusterStateReaderWriter.get());
            } else if (logger.isFineEnabled()) {
                logger.fine("Cluster wide hot restart status is: " + hotRestartStatus.get()
                        + " successful when load completion received from: " + sender);
            }

            sendClusterWideLoadCompletionResultIfAvailable(sender);
        } else if (Boolean.FALSE.equals(localLoadResult.get())) {
            if (hotRestartStatus.compareAndSet(PARTITION_TABLE_VERIFIED, VERIFICATION_FAILED)) {
                for (ClusterHotRestartEventListener listener : hotRestartEventListeners) {
                    listener.onHotRestartDataLoadComplete(VERIFICATION_FAILED);
                }
            }

            sendClusterWideLoadCompletionResultIfAvailable(sender);
        }
    }

    // operation thread
    void receiveHotRestartStatusFromMasterAfterLoadCompletion(Address sender, HotRestartClusterInitializationStatus result) {
        Address master = node.getMasterAddress();
        if (!master.equals(sender)) {
            logger.warning("Received load completion result from non-master member: " + sender + " master: " + master);
            return;
        }

        if (!(result == VERIFICATION_FAILED || result == VERIFICATION_AND_LOAD_SUCCEEDED)) {
            throw new IllegalArgumentException("Can not set hot restart status after load completion to " + result);
        }

        if (logger.isFineEnabled()) {
            logger.fine("Setting cluster-wide hot restart status " + hotRestartStatus.get() + " to " + result);
        }

        hotRestartStatus.compareAndSet(PARTITION_TABLE_VERIFIED, result);
    }

    // main & operation thread
    void setFinalClusterState(ClusterState newState) {
        logger.info("Setting final cluster state to: " + newState);
        setClusterState(node.getClusterService(), newState, true);
    }

    public boolean isStartWithHotRestart() {
        return startWithHotRestart;
    }

    public void onMembershipChange(MembershipServiceEvent event) {
        if (node.getClusterService().getClusterState() == ClusterState.ACTIVE) {
            writeMembers();
        }
    }

    private void writeMembers() {
        if (logger.isFineEnabled()) {
            logger.fine("Persisting member list...");
        }
        final ClusterServiceImpl clusterService = node.getClusterService();
        try {
            memberListWriter.write(clusterService.getMembers());
        } catch (IOException e) {
            logger.severe("While persisting member list", e);
        }
    }

    @Override
    public void replicaChanged(PartitionReplicaChangeEvent event) {
        if (logger.isFinestEnabled()) {
            logger.finest("Persisting partition table after " + event);
        }
        writePartitions();
    }

    private void writePartitions() {
        if (logger.isFinestEnabled()) {
            logger.finest("Persisting partition table...");
        }
        try {
            InternalPartitionService partitionService = node.getPartitionService();
            partitionTableWriter.setPartitionVersion(partitionService.getPartitionStateVersion());
            partitionTableWriter.write(partitionService.getPartitions());
        } catch (IOException e) {
            logger.severe("While persisting partition table", e);
        }
    }

    // operation thread
    public void onClusterStateChange(ClusterState newState) {
        if (logger.isFineEnabled()) {
            logger.fine("Persisting cluster state: " + newState);
        }
        try {
            clusterStateReaderWriter.write(newState);
        } catch (IOException e) {
            logger.severe("While persisting cluster state: " + newState, e);
        }
    }

    public HotRestartClusterInitializationStatus getHotRestartStatus() {
        return hotRestartStatus.get();
    }

    public void receiveForceStartFromMaster(final Address sender) {
        if (!sender.equals(node.getMasterAddress())) {
            logger.warning("Force restart command received from non-master member: " + sender);
            return;
        }

        final HotRestartClusterInitializationStatus status = hotRestartStatus.get();
        if (status == PENDING_VERIFICATION || status == PARTITION_TABLE_VERIFIED) {
            if (hotRestartStatus.compareAndSet(status, FORCE_STARTED)) {
                logger.info("Force start will proceed as it is received from master: " + sender);
                for (ClusterHotRestartEventListener listener : hotRestartEventListeners) {
                    listener.onForceStart();
                }
            } else {
                logger.warning("Could not set force start. Current: " + hotRestartStatus.get());
            }
        } else {
            logger.warning("Could not set force start since hot restart is already completed with: " + status);
        }
    }

    public boolean receiveForceStartTrigger(final Address sender) {
        if (!node.isMaster()) {
            logger.warning("Force start attempt received from " + sender + " but this node is not master!");
            return false;
        }

        final HotRestartClusterInitializationStatus status = hotRestartStatus.get();

        if (status == PENDING_VERIFICATION || status == PARTITION_TABLE_VERIFIED) {
            if (hotRestartStatus.compareAndSet(status, FORCE_STARTED)) {
                logger.info("Force start will proceed. Sender: " + sender);
                for (ClusterHotRestartEventListener listener : hotRestartEventListeners) {
                    listener.onForceStart();
                }

                sendOperationToOthers(new ForceStartMemberOperation());
                return true;
            } else {
                logger.warning("Could not set hot restart status to " + FORCE_STARTED + ". Current: " + hotRestartStatus.get());
            }
        }

        return false;
    }

    private void askForPartitionTableValidationStatus() {
        sendOperationToOthers(new AskForPartitionTableValidationStatusOperation());
    }

    private void askForLoadCompletionStatus() {
        sendOperationToOthers(new AskForLoadCompletionStatusOperation());
    }

    private void sendOperationToOthers(Operation operation) {
        final InternalOperationService operationService = node.nodeEngine.getOperationService();
        for (Address memberAddress : memberListRef.get()) {
            if (!node.getThisAddress().equals(memberAddress)) {
                operationService.send(operation, memberAddress);
            }
        }
    }


    private interface TimeoutableRunnable extends Runnable {
        void onTimeout();
    }

    private class ValidationTask implements TimeoutableRunnable {
        @Override
        public void run() {
            if (node.isMaster()) {
                askForPartitionTableValidationStatus();
            } else {
                sendPartitionTableToMaster();
            }
        }

        @Override
        public void onTimeout() {
            if (validationStartTime + validationTimeout < Clock.currentTimeMillis()) {
                throw new HotRestartException(
                        "Could not validate partition table, validation phase timed-out! "
                                + "Start: " + new Date(validationStartTime) + ", Deadline: "
                                + new Date(validationStartTime + validationTimeout)
                                + ", Timeout: " + TimeUnit.MILLISECONDS.toSeconds(validationTimeout) + " sec.");
            }
        }
    }

    private class LoadTask implements TimeoutableRunnable {
        private final boolean success;

        public LoadTask(boolean success) {
            this.success = success;
        }

        @Override
        public void run() {
            if (node.isMaster()) {
                askForLoadCompletionStatus();
            } else {
                sendLoadCompleteToMaster(success);
            }
        }

        @Override
        public void onTimeout() {
            throw new HotRestartException(
                    "Could not load hot-restart data, load phase timed-out! "
                            + "Start: " + new Date(loadStartTime) + ", Deadline: "
                            + new Date(loadStartTime + dataLoadTimeout)
                            + ", Timeout: " + TimeUnit.MILLISECONDS.toSeconds(dataLoadTimeout) + " sec.");
        }
    }
}
