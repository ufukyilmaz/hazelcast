package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.cluster.impl.ClusterServiceImpl;
import com.hazelcast.core.Member;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.partition.impl.PartitionListener;
import com.hazelcast.partition.impl.PartitionReplicaChangeEvent;
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
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.cluster.impl.ClusterStateManagerAccessor.addMembersRemovedInNotActiveState;
import static com.hazelcast.cluster.impl.ClusterStateManagerAccessor.setClusterState;

/**
 * ClusterMetadataManager is responsible from loading cluster metadata
 * (cluster state, member list and partition table) during restart phase,
 * validating these metadata cluster-wide before restoring actual data
 * and storing these metadata when they change during runtime.
 */
public final class ClusterMetadataManager
        implements PartitionListener {

    private static final String DIR_NAME = "cluster";
    private static final int SLEEP_MILLIS = 1000;

    private final Node node;
    private final MemberListWriter memberListWriter;
    private final PartitionTableWriter partitionTableWriter;
    private final ClusterStateReaderWriter clusterStateReaderWriter;
    private final File homeDir;
    private final ILogger logger;
    private final long VALIDATION_TIMEOUT = TimeUnit.MINUTES.toMillis(1);
    private final long LOAD_TIMEOUT = TimeUnit.MINUTES.toMillis(1);

    private final Result validatedClusterWide = new Result();
    private final Result loadedClusterWide = new Result();
    private volatile boolean startWithHotRestart = true;

    private long validationStartTime;
    private long loadStartTime;

    private final Set<Address> notValidatedAddresses = Collections.newSetFromMap(new ConcurrentHashMap<Address, Boolean>());

    private final Set<Address> notLoadedAddresses = Collections.newSetFromMap(new ConcurrentHashMap<Address, Boolean>());

    private final AtomicReference<Address[][]> partitionTableRef = new AtomicReference<Address[][]>();
    private final AtomicReference<Collection<Address>> memberListRef = new AtomicReference<Collection<Address>>();


    public ClusterMetadataManager(Node node, File hotRestartHome) {
        this.node = node;
        logger = node.getLogger(getClass());
        homeDir = new File(hotRestartHome, DIR_NAME);
        if (!homeDir.exists() && !homeDir.mkdirs()) {
            throw new HotRestartException("Cannot create " + homeDir.getAbsolutePath());
        }
        memberListWriter = new MemberListWriter(homeDir, node.getThisAddress());
        partitionTableWriter = new PartitionTableWriter(homeDir);
        clusterStateReaderWriter = new ClusterStateReaderWriter(homeDir);
    }

    // main thread
    public void prepare() {
        MemberListReader memberListReader = new MemberListReader(homeDir);
        int partitionCount = node.getGroupProperties().getInteger(GroupProperty.PARTITION_COUNT);
        PartitionTableReader partitionTableReader = new PartitionTableReader(homeDir, partitionCount);

        try {
            clusterStateReaderWriter.read();
            memberListReader.read();
            partitionTableReader.read();
        } catch (IOException e) {
            throw new HotRestartException(e);
        }

        Address thisAddress = memberListReader.getThisAddress();
        Collection<Address> addresses = memberListReader.getAddresses();
        Address[][] table = partitionTableReader.getTable();

        memberListRef.set(addresses);
        partitionTableRef.set(table);

        if (thisAddress == null && !addresses.isEmpty()) {
            throw new HotRestartException("Unexpected state! Could not load local member address from disk!");
        }

        if (thisAddress != null && !node.getThisAddress().equals(thisAddress)) {
            throw new HotRestartException("Wrong local address! Expected: " + node.getThisAddress() + ", Actual: " + thisAddress);
        }

        if (thisAddress == null) {
            logger.info("Cluster state not found on disk. Will not load hot-restart data.");
            startWithHotRestart = false;
        }

        notValidatedAddresses.addAll(addresses);
        notLoadedAddresses.addAll(addresses);

        if (startWithHotRestart) {
            final ClusterServiceImpl clusterService = node.clusterService;
            setClusterState(clusterService, ClusterState.PASSIVE);
            addMembersRemovedInNotActiveState(clusterService, addresses);
        }

        if (logger.isFineEnabled()) {
            logger.fine("Preparation completed...");
        }
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

        final int memberListSize = memberListRef.get().size();
        if (memberListSize <= 1) {
            logger.info("No need to start validation since member list size is " + memberListSize);
            validatedClusterWide.set(Result.SUCCESS);
            return;
        }

        logger.info("Starting cluster member-list & partition table validation.");

        if (startWithHotRestart) {
            awaitUntilAllMembersJoin();
            if (logger.isFineEnabled()) {
                logger.fine("All expected members joined...");
            }
        }

        notValidatedAddresses.remove(node.getThisAddress());

        waitFor(validatedClusterWide, new ValidationTask(), validationStartTime + VALIDATION_TIMEOUT);

        if (validatedClusterWide.isFailure()) {
            throw new HotRestartException("Cluster-wide validation failed!");
        }

        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) node.getPartitionService();
        partitionService.setInitialState(partitionTableRef.get());

        logger.info("Cluster member-list & partition table validation completed.");
    }

    // main thread
    private void awaitUntilAllMembersJoin() {
        final Collection<Address> loadedAddresses = memberListRef.get();
        final ClusterServiceImpl clusterService = node.getClusterService();
        Set<Member> members = clusterService.getMembers();
        while (members.size() != loadedAddresses.size()) {

            if (validationStartTime + VALIDATION_TIMEOUT < Clock.currentTimeMillis()) {
                throw new HotRestartException(
                        "Validation phase timed-out! " + "Start: " + new Date(validationStartTime) + ", Timeout: "
                                + VALIDATION_TIMEOUT);
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
                logger.fine("Failed to send partition table to master since master address is null");
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

        Address[][] localTable = partitionTableRef.get();
        InternalOperationService operationService = node.getNodeEngine().getOperationService();

        if (validatedClusterWide.isFailure()) {
            if (logger.isFineEnabled()) {
                logger.fine("Partition table validation is already failed. Sending failure to: " + sender);
            }
            operationService.send(new SendClusterValidationResultOperation(Result.FAILURE), sender);
            return;
        }

        boolean validated = Arrays.deepEquals(localTable, remoteTable);

        if (validated) {
            notValidatedAddresses.remove(sender);

            if (logger.isFineEnabled()) {
                logger.fine("Partition table validation successful for: " + sender + " not-validated: " + notValidatedAddresses);
            }

            if (notValidatedAddresses.isEmpty()) {
                validatedClusterWide.cas(Result.PENDING, Result.SUCCESS);

                final Result result = this.validatedClusterWide;

                if (logger.isFineEnabled()) {
                    logger.fine("Partition table validation completed for all members. " + "Sending " + result + " to: " + sender);
                }

                operationService.send(new SendClusterValidationResultOperation(result.get()), sender);
            }
        } else {
            validatedClusterWide.cas(Result.PENDING, Result.FAILURE);
            if (logger.isFineEnabled()) {
                logger.fine("Partition table validation failed for: " + sender + ", Current validation " + validatedClusterWide);
            }
            operationService.send(new SendClusterValidationResultOperation(Result.FAILURE), sender);
            // must be after validatedClusterWide.cas()!
            notValidatedAddresses.remove(sender);
        }
    }

    // operation thread
    void receiveClusterWideValidationResultFromMaster(int result) {
        if (logger.isFineEnabled()) {
            logger.fine("Setting cluster-wide validation status " + validatedClusterWide + " to " + result);
        }

        validatedClusterWide.set(result);
    }

    // main thread
    private void waitFor(Result result, TimeoutableRunnable runnable, long deadLine) {
        while (result.isPending()) {
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

        receiveLoadCompleteFromMember(node.getThisAddress(), success);

        waitFor(loadedClusterWide, new LoadTask(success), loadStartTime + LOAD_TIMEOUT);

        if (loadedClusterWide.isFailure()) {
            throw new HotRestartException("Cluster-wide load failed!", failure);
        }

        if (logger.isFineEnabled()) {
            logger.fine("Cluster-wide load completed...");
        }

        cleanupState();
        writeMembers();
        writePartitions();
    }

    // main thread
    private void cleanupState() {
        notValidatedAddresses.clear();
        notLoadedAddresses.clear();
        memberListRef.set(Collections.<Address>emptySet());
        partitionTableRef.set(null);
    }

    // main thread
    private void sendLoadCompleteToMaster(boolean success) {
        Address masterAddress = node.getMasterAddress();

        if (masterAddress == null) {
            if (logger.isFineEnabled()) {
                logger.fine("Failed to send load-completion status [" + success + "] to master since master address is null");
            }

            return;
        }

        if (logger.isFineEnabled()) {
            logger.fine("Sending load-completion status [" + success + "] to: " + masterAddress);
        }
        InternalOperationService operationService = node.getNodeEngine().getOperationService();
        Operation op = new SendLoadCompleteForValidationOperation(success);
        operationService.send(op, masterAddress);
    }

    // operation thread
    void receiveLoadCompleteFromMember(Address sender, boolean success) {
        if (logger.isFineEnabled()) {
            logger.fine("Received load-completion status [" + success + "] from: " + sender);
        }

        InternalOperationService operationService = node.getNodeEngine().getOperationService();

        if (!success) {
            loadedClusterWide.cas(Result.PENDING, Result.FAILURE);            

            if (!node.getThisAddress().equals(sender)) {
                if (logger.isFineEnabled()) {
                    logger.fine("Sending failure result to: " + sender + ", Current load-completion status: " + loadedClusterWide);
                }
                operationService.send(new SendClusterLoadResultOperation(Result.FAILURE, ClusterState.PASSIVE), sender);
            }
            notLoadedAddresses.remove(sender);
        } else if (loadedClusterWide.isPending()) {
            notLoadedAddresses.remove(sender);

            if (notLoadedAddresses.isEmpty()) {
				 loadedClusterWide.cas(Result.PENDING, Result.SUCCESS);                
                
                if (logger.isFineEnabled()) {
                    logger.fine("Cluster-wide load-completion status is: " + loadedClusterWide);
                }
            }
        }
        if (!loadedClusterWide.isPending()) {
            final ClusterState clusterState = clusterStateReaderWriter.get();
            if (!node.getThisAddress().equals(sender)) {
                if (logger.isFineEnabled()) {
                    logger.fine("Sending cluster-wide load-completion result " + loadedClusterWide + " to: " + sender);
                }
                operationService.send(new SendClusterLoadResultOperation(loadedClusterWide.get(), clusterState), sender);
            }
            if (loadedClusterWide.isSuccess()) {
                setFinalClusterState(clusterState);
            }
        }
    }

    // operation thread
    void receiveClusterWideLoadResultFromMaster(int result) {
        loadedClusterWide.set(result);
        if (logger.isFineEnabled()) {
            logger.fine("Setting cluster-wide load-completion status " + loadedClusterWide);
        }
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

    public void memberAdded(MemberImpl member) {
        writeMembers();
    }

    public void memberRemoved(MemberImpl member) {
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
            if (validationStartTime + VALIDATION_TIMEOUT < Clock.currentTimeMillis()) {
                throw new HotRestartException(
                        "Validation phase timed-out! " + "Start: " + new Date(validationStartTime) + ", Deadline: " + new Date(
                                validationStartTime + VALIDATION_TIMEOUT) + ", Timeout: " + VALIDATION_TIMEOUT);
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
                sendPartitionTableToMaster();
                sendLoadCompleteToMaster(success);
            }
        }

        @Override
        public void onTimeout() {
            throw new HotRestartException(
                    "Load phase timed-out! " + "Start: " + new Date(loadStartTime) + ", Deadline: " + new Date(
                            loadStartTime + LOAD_TIMEOUT) + ", Timeout: " + LOAD_TIMEOUT);
        }
    }
}
