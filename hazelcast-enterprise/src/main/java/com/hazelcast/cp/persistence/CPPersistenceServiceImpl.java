package com.hazelcast.cp.persistence;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.internal.CPMemberInfo;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.RaftInvocationManager;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.persistence.CPMetadataStore;
import com.hazelcast.cp.internal.persistence.CPPersistenceService;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.cp.internal.raft.impl.persistence.LogFileStructure;
import com.hazelcast.cp.internal.raft.impl.persistence.RaftStateStore;
import com.hazelcast.cp.internal.raft.impl.persistence.RestoredRaftState;
import com.hazelcast.cp.persistence.operation.PublishLocalCPMemberOp;
import com.hazelcast.cp.persistence.operation.PublishRestoredCPMembersOp;
import com.hazelcast.cp.persistence.raftop.VerifyRestartedCPMemberOp;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.DirectoryLock;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.properties.HazelcastProperty;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static com.hazelcast.cluster.memberselector.MemberSelectors.NON_LOCAL_MEMBER_SELECTOR;
import static com.hazelcast.cp.CPGroup.METADATA_CP_GROUP_NAME;
import static com.hazelcast.cp.persistence.CPMetadataStoreImpl.isCPDirectory;
import static com.hazelcast.cp.persistence.OnDiskRaftStateStore.MEMBERS_FILENAME;
import static com.hazelcast.cp.persistence.OnDiskRaftStateStore.TERM_FILENAME;
import static com.hazelcast.internal.nio.IOUtil.delete;
import static com.hazelcast.internal.util.DirectoryLock.lockForDirectory;
import static com.hazelcast.internal.util.FutureUtil.RETHROW_EVERYTHING;
import static com.hazelcast.internal.util.FutureUtil.waitWithDeadline;
import static com.hazelcast.internal.util.Preconditions.checkTrue;
import static com.hazelcast.internal.util.ThreadUtil.createThreadName;
import static com.hazelcast.spi.impl.executionservice.ExecutionService.ASYNC_EXECUTOR;
import static java.util.Arrays.sort;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Implementation of {@link CPPersistenceService}.
 * <p>
 * CPPersistenceService is responsible for;
 * <ul>
 *     <li>managing lifecycle of {@link RaftStateStore}s</li>
 *     <li>discovering and selecting persistence directories</li>
 *     <li>restoring persisted state and creating {@link RaftNodeImpl}s</li>
 * </ul>
 */
@SuppressWarnings({"checkstyle:classdataabstractioncoupling"})
public class CPPersistenceServiceImpl implements CPPersistenceService {

    /**
     * !!! ONLY FOR INTERNAL USAGE AND TESTING !!!
     * If enabled, a node tries to pick its own persistence directory if it discovers multiple directories.
     */
    public static final HazelcastProperty FAVOR_OWN_PERSISTENCE_DIRECTORY
            = new HazelcastProperty("hazelcast.cp.persistence.favor.own.directory", true);

    /**
     * Allow or disallow IP address change during restart.
     */
    public static final HazelcastProperty ALLOW_IP_ADDRESS_CHANGE
            = new HazelcastProperty("hazelcast.cp.persistence.allow.ip.change", true);

    private static final int PUBLISH_CP_MEMBERS_MILLIS = 250;

    private final Node node;
    private final File dir;
    private final CPMetadataStoreImpl metadataStore;
    private final ILogger logger;
    private final boolean allowIpAddressChange;
    private final DirectoryLock directoryLock;
    private final InternalSerializationService serializationService;
    private final CPSubsystemConfig cpSubsystemConfig;
    private volatile boolean startCompleted;
    private volatile CPMemberInfo localCPMember;

    public CPPersistenceServiceImpl(Node node) {
        this.node = node;
        this.logger = node.getLogger(getClass());
        this.serializationService = node.getSerializationService();
        this.allowIpAddressChange = node.getProperties().getBoolean(ALLOW_IP_ADDRESS_CHANGE);
        this.directoryLock = acquireDir(node.getConfig().getCPSubsystemConfig());
        this.dir = directoryLock.getDir();
        this.metadataStore = new CPMetadataStoreImpl(dir, serializationService);
        this.cpSubsystemConfig = node.getConfig().getCPSubsystemConfig();
    }

    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity"})
    private DirectoryLock acquireDir(CPSubsystemConfig config) {
        File baseDir = config.getBaseDir();
        if (!baseDir.exists() && !baseDir.mkdirs() && !baseDir.exists()) {
            throw new HazelcastException("Could not create " + baseDir.getAbsolutePath());
        }

        if (!baseDir.isDirectory()) {
            throw new HazelcastException(baseDir.getAbsolutePath() + " is not a directory!");
        }

        File[] dirs = baseDir.listFiles(f -> {
            if (f.isFile()) {
                return false;
            }

            boolean cpDirectory = isCPDirectory(f);
            if (!cpDirectory) {
                verifyNoCPGroupDirExists(f);
                logger.fine(f.getAbsolutePath() + " is not a valid CP data directory.");
            } else {
                try {
                    CPMember cpMember = new CPMetadataStoreImpl(f, serializationService).readLocalCPMember();
                    if (cpMember == null) {
                        verifyNoCPGroupDirExists(f);
                    }
                } catch (IOException e) {
                    throw new HazelcastException("Could not read local CP member file in " + f.getAbsolutePath(), e);
                }
            }

            return cpDirectory;
        });

        if (dirs == null) {
            return createNewDir(baseDir);
        }

        if (node.getProperties().getBoolean(FAVOR_OWN_PERSISTENCE_DIRECTORY)) {
            sort(dirs, new OwnDirComparator());
        }

        for (File dir : dirs) {
            try {
                if (!allowIpAddressChange) {
                    CPMember member = new CPMetadataStoreImpl(dir, serializationService).readLocalCPMember();
                    if (!node.getThisAddress().equals(member.getAddress())) {
                        logger.fine("This directory does not belong to us: " + dir.getAbsolutePath());
                        continue;
                    }
                }

                DirectoryLock directoryLock = lockForDirectory(dir, logger);
                logger.info("Found existing CP data directory: " + dir.getAbsolutePath());
                return directoryLock;
            } catch (Exception e) {
                logger.fine("Could not lock existing CP data directory: " + dir.getAbsolutePath()
                        + ". Reason: " + e.getMessage());
            }
        }

        // create a new persistence directory
        return createNewDir(baseDir);
    }

    private DirectoryLock createNewDir(File baseDir) {
        File dir = new File(baseDir, UuidUtil.newUnsecureUuidString());
        boolean created = dir.mkdir();
        assert created : "Couldn't create " + dir.getAbsolutePath();
        logger.info("Created new empty CP data directory: " + dir.getAbsolutePath());
        return lockForDirectory(dir, logger);
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public CPMetadataStore getCPMetadataStore() {
        return metadataStore;
    }

    @Nonnull
    public RaftStateStore createRaftStateStore(@Nonnull RaftGroupId groupId, @Nullable LogFileStructure logFileStructure) {
        File groupDir = getGroupDir(groupId);
        int maxUncommittedEntries = cpSubsystemConfig.getRaftAlgorithmConfig().getUncommittedEntryCountToRejectNewAppends() + 1;
        return new OnDiskRaftStateStore(groupDir, node.getSerializationService(), maxUncommittedEntries, logFileStructure);
    }

    @Override
    public void removeRaftStateStore(@Nonnull RaftGroupId groupId) {
         delete(getGroupDir(groupId));
    }

    @Override
    public void reset() {
        File[] files = dir.listFiles((dir, name) -> !DirectoryLock.FILE_NAME.equals(name));
        if (files != null) {
            // files are deleted in the following order:
            // 1. cp member file
            // 2. cp group dirs
            // 3. metadata group id file

            Arrays.sort(files, (f1, f2) -> {
                if (CPMetadataStoreImpl.isCPMemberFile(dir, f1.getName())) {
                    return -1;
                } else if (CPMetadataStoreImpl.isCPMemberFile(dir, f2.getName())) {
                    return 1;
                } else if (CPMetadataStoreImpl.isMetadataGroupIdFile(dir, f1.getName())) {
                    return 1;
                } else if (CPMetadataStoreImpl.isMetadataGroupIdFile(dir, f2.getName())) {
                    return -1;
                }

                return 0;
            });

            for (File f : files) {
                delete(f);
            }
        }
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
    public void start() {
        logger.info("Starting CP restore process in " + dir.getAbsolutePath());

        RaftService raftService = node.getNodeEngine().getService(RaftService.SERVICE_NAME);
        try {
            RaftGroupId metadataGroupId = metadataStore.readMetadataGroupId();
            if (metadataGroupId != null) {
                raftService.getMetadataGroupManager().restoreMetadataGroupId(metadataGroupId);
            }
        } catch (IOException e) {
            throw new HazelcastException(e);
        }

        boolean addressChangeDetected;
        try {
            localCPMember = metadataStore.readLocalCPMember();
            if (localCPMember == null) {
                verifyNoCPGroupDirExists(dir);
                logger.info("Nothing to restore in " + dir.getAbsolutePath());
                startCompleted = true;
                return;
            }

            addressChangeDetected = !node.getThisAddress().equals(localCPMember.getAddress());
            if (addressChangeDetected) {
                logger.warning("IP address change detected! " + localCPMember.getAddress() + " -> " + node.getThisAddress());
                assert allowIpAddressChange : "IP address change is now allowed!";
                localCPMember = new CPMemberInfo(localCPMember.getUuid(), node.getThisAddress());
            }
        } catch (IOException e) {
            throw new HazelcastException(e);
        }

        List<Future<Void>> futures = new ArrayList<>();
        CompletableFuture<Void> verificationFuture = new InternalCompletableFuture<>();
        futures.add(verificationFuture);
        if (addressChangeDetected) {
            runAsync("cp-local-member-address-publish-thread", () -> publishLocalAddressChange(verificationFuture));
        }

        runAsync("notify-metadata-group-thread", () -> verifyRestartedCPMember(verificationFuture));

        File[] groupDirs = getGroupDirs(dir);
        if (groupDirs == null || groupDirs.length == 0) {
            logger.info("No CP group to restore in " + dir.getAbsolutePath());
        } else {
            ExecutionService executionService = node.getNodeEngine().getExecutionService();
            for (File groupDir : groupDirs) {
                Future<Void> f = executionService.submit(ASYNC_EXECUTOR, new RestoreCPGroupTask(groupDir, raftService));
                futures.add(f);
            }
        }

        waitWithDeadline(futures, cpSubsystemConfig.getDataLoadTimeoutSeconds(), SECONDS, RETHROW_EVERYTHING);

        this.startCompleted = true;
        logger.fine("CP restore completed...");
    }

    private void verifyNoCPGroupDirExists(File dir) {
        checkTrue(dir.isDirectory(), dir.getAbsolutePath() + " is not a directory!");
        File[] groupDirs = getGroupDirs(dir);
        if (groupDirs != null && groupDirs.length > 0) {
            List<String> invalidDirNames = Arrays.stream(groupDirs)
                                                 .map(File::getName)
                                                 .collect(Collectors.toList());

            throw new IllegalStateException(dir.getAbsolutePath() + " contains CP group directories: " + invalidDirNames
                    + " without CP member identity file!");
        }
    }

    private void publishLocalAddressChange(Future<Void> futureToCheck) {
        NodeEngineImpl nodeEngine = node.getNodeEngine();
        OperationService operationService = nodeEngine.getOperationService();
        ClusterService clusterService = nodeEngine.getClusterService();
        RaftService raftService = node.getNodeEngine().getService(RaftService.SERVICE_NAME);

        while (!futureToCheck.isDone()) {
            raftService.getInvocationManager().getRaftInvocationContext().updateMember(localCPMember);

            logger.fine("Broadcasting local CP member... " + localCPMember);
            Operation op = new PublishLocalCPMemberOp(localCPMember);
            for (Member member : clusterService.getMembers(NON_LOCAL_MEMBER_SELECTOR)) {
                operationService.send(op, member.getAddress());
            }

            try {
                Thread.sleep(SECONDS.toMillis(1));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void verifyRestartedCPMember(CompletableFuture<Void> future) {
        RaftService raftService = node.getNodeEngine().getService(RaftService.SERVICE_NAME);
        RaftInvocationManager invocationManager = raftService.getInvocationManager();
        try {
            // METADATA group id is already restored...
            invocationManager.invoke(raftService.getMetadataGroupId(), new VerifyRestartedCPMemberOp(localCPMember)).join();
            logger.info("CP member is verified on the METADATA group.");
            // setting local cp member after my ip change is committed to the metadata group
            // because we initialize local cp member also when the initial cp discovery is completed
            // so we use the same ordering logic here.
            raftService.getMetadataGroupManager().restoreLocalCPMember(localCPMember);

            // if this call fails and I restart again, I will re-commit my ip change
            // to the metadata group without any problem.
            metadataStore.persistLocalCPMember(localCPMember);
            future.complete(null);
        } catch (Throwable t) {
            future.completeExceptionally(t);
            logger.severe("Could not verify the CP member on the METADATA group", t);
        }
    }

    public void shutdown() {
        directoryLock.release();
    }

    public boolean isStartCompleted() {
        return startCompleted;
    }

    public File getGroupDir(RaftGroupId groupId) {
        return new File(dir, groupId.getName() + "@" + groupId.getSeed() + "@" + groupId.getId());
    }

    private RaftGroupId getGroupId(File groupDir) {
        String[] split = groupDir.getName().split("@");
        if (split.length != 3) {
            throw new IllegalArgumentException("Invalid CP group persistence directory: " + groupDir.getName());
        }

        return new RaftGroupId(split[0], Long.parseLong(split[1]), Long.parseLong(split[2]));
    }

    private File[] getGroupDirs(File dir) {
        return dir.listFiles(path ->
                path.isDirectory() && new File(path, MEMBERS_FILENAME).exists() && new File(path, TERM_FILENAME).exists());
    }

    private void runAsync(String taskName, Runnable runnable) {
        String threadName = createThreadName(node.hazelcastInstance.getName(), taskName);
        new Thread(runnable, threadName).start();
    }

    private class RestoreCPGroupTask implements Callable<Void> {
        private final File groupDir;
        private final RaftService raftService;

        RestoreCPGroupTask(File groupDir, RaftService raftService) {
            this.groupDir = groupDir;
            this.raftService = raftService;
        }

        @Override
        public Void call() throws Exception {
            final RaftGroupId groupId = getGroupId(groupDir);
            logger.info("Restoring " + groupId + " from " + groupDir);
            int uncommittedEntryCount = cpSubsystemConfig.getRaftAlgorithmConfig().getUncommittedEntryCountToRejectNewAppends();
            OnDiskRaftStateLoader stateLoader =
                    new OnDiskRaftStateLoader(groupDir, uncommittedEntryCount + 1, node.getSerializationService());
            final RestoredRaftState restoredState = stateLoader.load();

            if (!localCPMember.getUuid().equals(restoredState.localEndpoint().getUuid())) {
                 throw new IllegalStateException("Local CP member: " + localCPMember + ", restored endpoint: "
                         + restoredState.localEndpoint() + ", group: " + groupId);
            }

            RaftNodeImpl raftNode = raftService.restoreRaftNode(groupId, restoredState, stateLoader.logFileStructure());

            if (groupId.getName().equals(METADATA_CP_GROUP_NAME)) {
                ArrayList<CPMember> members = new ArrayList<>();
                try {
                    long commitIndex = metadataStore.readActiveCPMembers(members);
                    if (members.isEmpty()) {
                        logger.warning("Restored active CP members list is empty with commitIndex: " + commitIndex);
                    }
                    replaceCPMemberIfIPChanged(members);

                    runAsync("cp-metadata-restore-thread",
                            () -> publishCPMembersUntilMetadataGroupLeaderElected(groupId, raftNode, members, commitIndex));
                } catch (Exception e) {
                    logger.severe(e);
                    throw e;
                }
            }

            logger.info("Completed restore of " + groupId);
            return null;
        }

        private void replaceCPMemberIfIPChanged(ArrayList<CPMember> members) {
            CPMemberInfo member = localCPMember;
            for (int i = 0; i < members.size(); i++) {
                CPMember m = members.get(i);
                if (m.getUuid().equals(member.getUuid()) && !m.getAddress().equals(member.getAddress())) {
                    members.set(i, member);
                    break;
                }
            }
        }

        private void publishCPMembersUntilMetadataGroupLeaderElected(RaftGroupId metadataGroupId, RaftNodeImpl raftNode,
                Collection<CPMember> cpMembers, long membersCommitIndex) {
            updateInvocationManager(metadataGroupId, membersCommitIndex, cpMembers);

            long timeout = SECONDS.toMillis(cpSubsystemConfig.getDataLoadTimeoutSeconds());
            long deadline = Clock.currentTimeMillis() + timeout;

            ClusterService clusterService = node.getClusterService();
            OperationServiceImpl operationService = node.getNodeEngine().getOperationService();
            Operation op = new PublishRestoredCPMembersOp(metadataGroupId, membersCommitIndex, cpMembers);

            while (Clock.currentTimeMillis() < deadline && raftNode.getLeader() == null) {
                if (logger.isFineEnabled()) {
                    logger.fine("Broadcasting restored CP members list...");
                }

                for (Member member : clusterService.getMembers(NON_LOCAL_MEMBER_SELECTOR)) {
                    operationService.send(op, member.getAddress());
                }
                try {
                    Thread.sleep(PUBLISH_CP_MEMBERS_MILLIS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }

            if (raftNode.getLeader() == null) {
                logger.severe("Metadata CP group leader election could not not be completed in time during recovery...");
            }
        }

        private void updateInvocationManager(RaftGroupId metadataGroupId, long membersCommitIndex,
                                             Collection<CPMember> members) {
            raftService.updateInvocationManagerMembers(metadataGroupId.getSeed(), membersCommitIndex, members);
            if (logger.isFineEnabled()) {
                logger.fine("Restored seed: " + metadataGroupId.getSeed() + ", members commit index: " + membersCommitIndex
                        + ", CP member list: " + members);
            }
        }
    }

    private class OwnDirComparator implements Comparator<File> {
        @Override
        public int compare(File dir1, File dir2) {
            try {
                CPMember member1 = new CPMetadataStoreImpl(dir1, serializationService).readLocalCPMember();
                CPMember member2 = new CPMetadataStoreImpl(dir2, serializationService).readLocalCPMember();
                if (member1 == null) {
                    return member2 != null ? 1 : 0;
                }
                if (member2 == null) {
                    return -1;
                }

                Address thisAddress = node.getThisAddress();
                if (thisAddress.equals(member1.getAddress())) {
                    return -1;
                }
                return thisAddress.equals(member2.getAddress()) ? 1 : 0;
            } catch (IOException e) {
                logger.warning("Could not compare addresses in CP Subsystem persistence directories: "
                        + dir1.getAbsolutePath() + " and " + dir2.getAbsolutePath(), e);
                return 0;
            }
        }
    }
}
