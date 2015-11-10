package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.cluster.impl.ClusterServiceImpl;
import com.hazelcast.config.HotRestartConfig;
import com.hazelcast.core.Member;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.partition.impl.PartitionListener;
import com.hazelcast.partition.impl.PartitionReplicaChangeEvent;
import com.hazelcast.spi.MembershipServiceEvent;
import com.hazelcast.spi.Operation;
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

import static com.hazelcast.cluster.impl.ClusterStateManagerAccessor.addMembersRemovedInNotActiveState;
import static com.hazelcast.cluster.impl.ClusterStateManagerAccessor.setClusterState;
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
@SuppressWarnings("checkstyle:classdataabstractioncoupling")
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

    private final AtomicReference<Address[][]> partitionTableRef = new AtomicReference<Address[][]>();
    private final AtomicReference<Collection<Address>> memberListRef = new AtomicReference<Collection<Address>>();

    private final List<ClusterHotRestartEventListener> hotRestartEventListeners =
            new CopyOnWriteArrayList<ClusterHotRestartEventListener>();

    public ClusterMetadataManager(Node node, File hotRestartHome, HotRestartConfig hotRestartConfig) {
        this.node = node;
        logger = node.getLogger(getClass());
        homeDir = new File(hotRestartHome, DIR_NAME);
        if (!homeDir.exists() && !homeDir.mkdirs()) {
            throw new HotRestartException("Cannot create " + homeDir.getAbsolutePath());
        }
        validationTimeout = TimeUnit.SECONDS.toMillis(hotRestartConfig.getValidationTimeoutSeconds());
        dataLoadTimeout = TimeUnit.SECONDS.toMillis(hotRestartConfig.getDataLoadTimeoutSeconds());
        memberListWriter = new MemberListWriter(homeDir, node.getThisAddress());
        partitionTableWriter = new PartitionTableWriter(homeDir);
        clusterStateReaderWriter = new ClusterStateReaderWriter(homeDir);
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
                setClusterState(clusterService, ClusterState.PASSIVE);
                addMembersRemovedInNotActiveState(clusterService, addresses);
            }

            for (ClusterHotRestartEventListener listener : hotRestartEventListeners) {
                listener.onPrepareComplete(addresses, table, startWithHotRestart);
            }
        } catch (IOException e) {
            throw new HotRestartException(e);
        }

        if (logger.isFineEnabled()) {
            logger.fine("Preparation completed...");
        }
    }

    private Address[][] restorePartitionTable() throws IOException {
        int partitionCount = node.getGroupProperties().getInteger(GroupProperty.PARTITION_COUNT);
        PartitionTableReader partitionTableReader = new PartitionTableReader(homeDir, partitionCount);
        partitionTableReader.read();
        Address[][] table = partitionTableReader.getTable();
        partitionTableRef.set(table);
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
        node.partitionService.addPartitionListener(this);
        loadStartTime = Clock.currentTimeMillis();
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
            if (logger.isFineEnabled()) {
                logger.fine("All expected members joined...");
            }
        }

        for (ClusterHotRestartEventListener listener : hotRestartEventListeners) {
            listener.onAllMembersJoin(memberListRef.get());
        }

        notValidatedAddresses.remove(node.getThisAddress());

        EnumSet<HotRestartClusterInitializationStatus> statusses = EnumSet
                .of(VERIFICATION_FAILED, PARTITION_TABLE_VERIFIED, VERIFICATION_AND_LOAD_SUCCEEDED);
        waitForFailureOrExpectedStatus(statusses, new ValidationTask(), validationStartTime + validationTimeout);

        final HotRestartClusterInitializationStatus status = hotRestartStatus.get();
        for (ClusterHotRestartEventListener listener : hotRestartEventListeners) {
            listener.onPartitionTableValidationComplete(status);
        }

        if (status == VERIFICATION_FAILED) {
            throw new HotRestartException("Cluster-wide validation failed!");
        }

        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) node.getPartitionService();
        partitionService.setInitialState(partitionTableRef.get());

        logger.info("Cluster member-list & partition table validation completed.");
    }

    private boolean completeValidationIfSingleMember() {
        final int memberListSize = memberListRef.get().size();
        if (memberListSize <= 1) {
            logger.info("No need to start validation since member list size is " + memberListSize);
            hotRestartStatus.set(VERIFICATION_AND_LOAD_SUCCEEDED);
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
                        "Validation phase timed-out! " + "Start: " + new Date(validationStartTime) + ", Timeout: "
                                + validationTimeout);
            }

            logger.info("Waiting for cluster formation... Expected: " + loadedAddresses.size() + ", Actual: " + members.size());

            sleep1s();
            members = clusterService.getMembers();
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
            if (logger.isFineEnabled()) {
                logger.fine("Failed to send partition table to master since master address is null.");
            }

            return;
        } else if (masterAddress.equals(node.getThisAddress())) {
            if (logger.isFineEnabled()) {
                logger.fine("Failed to send partition table to master since this node became master just now.");
            }
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
        if (logger.isFineEnabled()) {
            logger.fine("Received partition table from " + sender);
        }
        if (!node.isMaster()) {
            return;
        }

        if (hotRestartStatus.get() == VERIFICATION_FAILED) {
            if (logger.isFineEnabled()) {
                logger.fine("Partition table validation is already failed. Sending failure to: " + sender);
            }
            InternalOperationService operationService = node.getNodeEngine().getOperationService();
            operationService.send(new SendPartitionTableValidationResultOperation(VERIFICATION_FAILED), sender);
            return;
        }

        Address[][] localTable = partitionTableRef.get();
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

    private void processSuccessfulPartitionTableValidation(Address sender) {
        InternalOperationService operationService = node.getNodeEngine().getOperationService();

        notValidatedAddresses.remove(sender);
        if (logger.isFineEnabled()) {
            logger.fine("Partition table validation successful for: " + sender + " not-validated: " + notValidatedAddresses);
        }

        if (notValidatedAddresses.isEmpty()) {
            hotRestartStatus.compareAndSet(PENDING_VERIFICATION, PARTITION_TABLE_VERIFIED);

            final HotRestartClusterInitializationStatus result = hotRestartStatus.get();
            if (logger.isFineEnabled()) {
                logger.fine("Partition table validation completed for all members. Sending " + result
                        + " to: " + sender);
            }
            operationService.send(new SendPartitionTableValidationResultOperation(result), sender);
        }
    }

    private void processFailedPartitionTableValidation(Address sender) {
        InternalOperationService operationService = node.getNodeEngine().getOperationService();

        if (hotRestartStatus.get() == VERIFICATION_AND_LOAD_SUCCEEDED) {
            logger.warning("Wrong partition table received from " + sender
                    + " after load successfully completed cluster-wide");
            operationService.send(new SendPartitionTableValidationResultOperation(VERIFICATION_FAILED), sender);
            return;
        }

        hotRestartStatus.set(VERIFICATION_FAILED);
        if (logger.isFineEnabled()) {
            logger.fine("Partition table validation failed for: " + sender + ", Current validation " + VERIFICATION_FAILED);
        }

        operationService.send(new SendPartitionTableValidationResultOperation(VERIFICATION_FAILED), sender);
        // must be removed after hotRestartStatus.set(VERIFICATION_FAILED)!
        notValidatedAddresses.remove(sender);
    }

    // operation thread
    void receiveHotRestartStatusFromMasterAfterPartitionTableVerification(HotRestartClusterInitializationStatus result) {
        if (result == PENDING_VERIFICATION) {
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
        boolean success = failure == null;
        if (logger.isFineEnabled()) {
            logger.fine("Local hot-restart load completed: " + success + ". Waiting for all members to load...");
        }

        receiveLoadCompletionStatusFromMember(node.getThisAddress(), success);

        EnumSet<HotRestartClusterInitializationStatus> statusses = EnumSet
                .of(VERIFICATION_FAILED, VERIFICATION_AND_LOAD_SUCCEEDED);
        waitForFailureOrExpectedStatus(statusses, new LoadTask(success), loadStartTime + dataLoadTimeout);

        final HotRestartClusterInitializationStatus status = hotRestartStatus.get();
        for (ClusterHotRestartEventListener listener : hotRestartEventListeners) {
            listener.onHotRestartDataLoadComplete(status);
        }

        if (status == VERIFICATION_FAILED) {
            throw new HotRestartException("Cluster-wide load failed!", failure);
        }

        if (logger.isFineEnabled()) {
            logger.fine("Cluster-wide load completed...");
        }

        writeMembers();
        writePartitions();
    }

    // main thread
    private void sendLoadCompleteToMaster(boolean success) {
        Address masterAddress = node.getMasterAddress();

        if (masterAddress == null) {
            if (logger.isFineEnabled()) {
                logger.fine("Failed to send load-completion status [" + success + "] to master since master address is null");
            }

            return;
        } else if (masterAddress.equals(node.getThisAddress())) {
            if (logger.isFineEnabled()) {
                logger.fine("Failed to send load-completion status [" + success + "] to master since this node is master");
            }

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

        InternalOperationService operationService = node.getNodeEngine().getOperationService();

        for (ClusterHotRestartEventListener listener : hotRestartEventListeners) {
            listener.onHotRestartDataLoadResult(sender, success);
        }

        if (!success) {
            if (hotRestartStatus.get() == VERIFICATION_AND_LOAD_SUCCEEDED) {
                logger.warning("Failed load status received from " + sender + " after load successfully completed cluster-wide");
                operationService.send(new SendLoadCompletionStatusOperation(VERIFICATION_FAILED, ClusterState.PASSIVE), sender);
                return;
            }

            processFailedLoadCompletionStatus(sender);

        } else if (hotRestartStatus.get() == PARTITION_TABLE_VERIFIED) {
            processSuccessfulLoadCompletionStatusWhenPartitionTableVerified(sender);
        }

        sendClusterWideLoadCompletionResultIfAvailable(sender);
    }

    private void sendClusterWideLoadCompletionResultIfAvailable(Address sender) {
        InternalOperationService operationService = node.getNodeEngine().getOperationService();
        final HotRestartClusterInitializationStatus statusAfterReceive = hotRestartStatus.get();
        if (statusAfterReceive == VERIFICATION_AND_LOAD_SUCCEEDED || statusAfterReceive == VERIFICATION_FAILED) {
            final ClusterState clusterState = clusterStateReaderWriter.get();
            if (!node.getThisAddress().equals(sender)) {
                if (logger.isFineEnabled()) {
                    logger.fine("Sending cluster-wide load-completion result " + statusAfterReceive + " to: " + sender);
                }
                operationService.send(new SendLoadCompletionStatusOperation(statusAfterReceive, clusterState), sender);
            }
            if (statusAfterReceive == VERIFICATION_AND_LOAD_SUCCEEDED) {
                setFinalClusterState(clusterState);
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
            if (logger.isFineEnabled()) {
                logger.fine("Sending failure result to: " + sender + ", Current load-completion status: "
                        + hotRestartStatus.get());
            }
            operationService.send(new SendLoadCompletionStatusOperation(VERIFICATION_FAILED, ClusterState.PASSIVE), sender);
        }

        notLoadedAddresses.remove(sender);
    }

    private void processSuccessfulLoadCompletionStatusWhenPartitionTableVerified(Address sender) {
        notLoadedAddresses.remove(sender);

        if (notLoadedAddresses.isEmpty()) {
            if (hotRestartStatus.compareAndSet(PARTITION_TABLE_VERIFIED, VERIFICATION_AND_LOAD_SUCCEEDED)) {
                for (ClusterHotRestartEventListener listener : hotRestartEventListeners) {
                    listener.onHotRestartDataLoadComplete(VERIFICATION_AND_LOAD_SUCCEEDED);
                }
            }

            if (logger.isFineEnabled()) {
                logger.fine("Cluster-wide load-completion status is: " + hotRestartStatus.get());
            }
        }
    }

    // operation thread
    void receiveHotRestartStatusFromMasterAfterLoadCompletion(HotRestartClusterInitializationStatus result) {
        if (!(result == VERIFICATION_FAILED || result == VERIFICATION_AND_LOAD_SUCCEEDED)) {
            throw new IllegalArgumentException("Can not set hot restart status after load completion to " + result);
        }

        if (logger.isFineEnabled()) {
            logger.fine("Setting cluster-wide load-completion status " + hotRestartStatus.get() + " to " + result);
        }

        hotRestartStatus.compareAndSet(PARTITION_TABLE_VERIFIED, result);
    }

    // main & operation thread
    void setFinalClusterState(ClusterState newState) {
        if (logger.isFineEnabled()) {
            logger.fine("Setting final cluster state to: " + newState);
        }
        setClusterState(node.getClusterService(), newState);
    }

    public boolean isStartWithHotRestart() {
        return startWithHotRestart;
    }

    public void onMembershipChange(MembershipServiceEvent event) {
        writeMembers();
    }

    private void writeMembers() {
        if (logger.isFinestEnabled()) {
            logger.finest("Persisting member list...");
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
            partitionTableWriter.write(node.getPartitionService().getPartitions());
        } catch (IOException e) {
            logger.severe("While persisting partition table", e);
        }
    }

    // operation thread
    public void onClusterStateChange(ClusterState newState) {
        if (logger.isFinestEnabled()) {
            logger.finest("Persisting cluster state: " + newState);
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

    private interface TimeoutableRunnable
            extends Runnable {
        void onTimeout();
    }

    private class ValidationTask
            implements TimeoutableRunnable {
        @Override
        public void run() {
            if (!node.isMaster()) {
                sendPartitionTableToMaster();
            }
        }

        @Override
        public void onTimeout() {
            if (validationStartTime + validationTimeout < Clock.currentTimeMillis()) {
                throw new HotRestartException(
                        "Validation phase timed-out! " + "Start: " + new Date(validationStartTime) + ", Deadline: "
                                + new Date(validationStartTime + validationTimeout) + ", Timeout: " + validationTimeout);
            }
        }
    }

    private class LoadTask
            implements TimeoutableRunnable {
        private final boolean success;

        public LoadTask(boolean success) {
            this.success = success;
        }

        @Override
        public void run() {
            if (!node.isMaster()) {
                sendLoadCompleteToMaster(success);
            }
        }

        @Override
        public void onTimeout() {
            throw new HotRestartException(
                    "Load phase timed-out! " + "Start: " + new Date(loadStartTime) + ", Deadline: "
                            + new Date(loadStartTime + dataLoadTimeout) + ", Timeout: " + dataLoadTimeout);
        }
    }
}
