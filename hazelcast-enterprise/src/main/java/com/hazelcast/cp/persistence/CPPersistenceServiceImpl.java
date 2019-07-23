package com.hazelcast.cp.persistence;

import com.hazelcast.cluster.Member;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.config.cp.RaftAlgorithmConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.cp.internal.CPMemberInfo;
import com.hazelcast.cp.internal.MetadataRaftGroupSnapshot;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.persistence.CPMetadataStore;
import com.hazelcast.cp.internal.persistence.CPPersistenceService;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.cp.internal.raft.impl.log.LogEntry;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotEntry;
import com.hazelcast.cp.internal.raft.impl.persistence.LogFileStructure;
import com.hazelcast.cp.internal.raft.impl.persistence.RaftStateStore;
import com.hazelcast.cp.internal.raft.impl.persistence.RestoredRaftState;
import com.hazelcast.cp.internal.raftop.metadata.AddCPMemberOp;
import com.hazelcast.cp.internal.raftop.metadata.InitMetadataRaftGroupOp;
import com.hazelcast.cp.internal.raftop.metadata.RemoveCPMemberOp;
import com.hazelcast.cp.internal.raftop.snapshot.RestoreSnapshotOp;
import com.hazelcast.cp.persistence.operation.PublishRestoredCPMembersOp;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.DirectoryLock;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cluster.memberselector.MemberSelectors.NON_LOCAL_MEMBER_SELECTOR;
import static com.hazelcast.cp.CPGroup.METADATA_CP_GROUP_NAME;
import static com.hazelcast.cp.persistence.CPMemberMetadataStoreImpl.CP_METADATA_FILE_NAME;
import static com.hazelcast.internal.nio.IOUtil.delete;
import static com.hazelcast.internal.util.DirectoryLock.lockForDirectory;
import static com.hazelcast.internal.util.FutureUtil.RETHROW_EVERYTHING;
import static com.hazelcast.internal.util.FutureUtil.waitWithDeadline;
import static com.hazelcast.internal.util.ThreadUtil.createThreadName;
import static com.hazelcast.spi.impl.executionservice.ExecutionService.ASYNC_EXECUTOR;

public class CPPersistenceServiceImpl implements CPPersistenceService {

    private final Node node;
    private final File dir;
    private final CPMemberMetadataStoreImpl cpMemberMetadataStore;
    private final ILogger logger;
    private final DirectoryLock directoryLock;
    private final int uncommittedEntryCount;
    private volatile boolean startCompleted;
    private volatile CPMemberInfo localMember;

    public CPPersistenceServiceImpl(Node node) {
        this.node = node;
        this.logger = node.getLogger(getClass());
        this.directoryLock = acquireDir(node.getConfig().getCPSubsystemConfig());
        this.dir = directoryLock.getDir();
        this.cpMemberMetadataStore = new CPMemberMetadataStoreImpl(dir);
        RaftAlgorithmConfig config = node.getConfig().getCPSubsystemConfig().getRaftAlgorithmConfig();
        this.uncommittedEntryCount = config.getUncommittedEntryCountToRejectNewAppends();
    }

    private DirectoryLock acquireDir(CPSubsystemConfig config) {
        File baseDir = config.getBaseDir();
        if (!baseDir.exists() && !baseDir.mkdirs() && !baseDir.exists()) {
            throw new HazelcastException("Could not create " + baseDir.getAbsolutePath());
        }
        if (!baseDir.isDirectory()) {
            throw new HazelcastException(baseDir.getAbsolutePath() + " is not a directory!");
        }
        File[] dirs = baseDir.listFiles(new FileFilter() {
            @Override
            public boolean accept(File f) {
                boolean cpDirectory = isCPDirectory(f);
                if (!cpDirectory) {
                    logger.fine(f.getAbsolutePath() + " is not a valid CP data directory.");
                }
                return cpDirectory;
            }
        });
        if (dirs == null) {
            return createNewDir(baseDir);
        }
        for (File dir : dirs) {
            try {
                CPMemberInfo member = new CPMemberMetadataStoreImpl(dir).readLocalMember();
                if (!node.getThisAddress().equals(member.getAddress())) {
                    logger.fine("This directory does not belong to us: " + dir.getAbsolutePath());
                    continue;
                }

                DirectoryLock directoryLock = lockForDirectory(dir, logger);
                logger.info("Found existing CP data directory: " + dir.getAbsolutePath());
                return directoryLock;
            } catch (Exception e) {
                logger.fine("Could not lock existing CP data directory: " + dir.getAbsolutePath()
                        + ". Reason: " + e.getMessage());
            }
        }
        // create a new one
        return createNewDir(baseDir);
    }

    private DirectoryLock createNewDir(File baseDir) {
        File dir = new File(baseDir, UuidUtil.newUnsecureUuidString());
        boolean created = dir.mkdir();
        assert created : "Couldn't create " + dir.getAbsolutePath();
        logger.info("Created new empty CP data directory: " + dir.getAbsolutePath());
        return lockForDirectory(dir, logger);
    }

    private static boolean isCPDirectory(File dir) {
        return new File(dir, CP_METADATA_FILE_NAME).exists();
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public CPMetadataStore getCPMetadataStore() {
        return cpMemberMetadataStore;
    }

    @Nonnull
    public RaftStateStore createRaftStateStore(
            @Nonnull RaftGroupId groupId, @Nullable LogFileStructure logFileStructure
    ) {
        File groupDir = getGroupDir(groupId);
        if (!groupDir.exists() && !groupDir.mkdir() && !groupDir.exists()) {
            throw new IllegalStateException("Cannot create directory " + groupDir.getAbsolutePath() + " for " + groupId);
        }
        return new OnDiskRaftStateStore(groupDir, node.getSerializationService(), uncommittedEntryCount + 1, logFileStructure);
    }

    @Override
    public void reset() {
        File[] files = dir.listFiles();
        if (files != null) {
            for (File f : files) {
                delete(f);
            }
        }
    }

    public void start() {
        logger.info("Starting CP restore process in " + dir.getAbsolutePath());
        File[] groupDirs = getGroupDirs();
        if (groupDirs == null || groupDirs.length == 0) {
            logger.info("Nothing to restore in " + dir.getAbsolutePath());
            startCompleted = true;
            return;
        }

        try {
            localMember = cpMemberMetadataStore.readLocalMember();
        } catch (IOException e) {
            throw new HazelcastException(e);
        }
        assert node.getThisAddress().equals(localMember.getAddress())
                : "Local address: " + node.getThisAddress() + ", CP member: " + localMember;

        RaftService raftService = node.getNodeEngine().getService(RaftService.SERVICE_NAME);
        ExecutionService executionService = node.getNodeEngine().getExecutionService();
        List<Future<Void>> futures = new ArrayList<Future<Void>>(groupDirs.length);
        for (File groupDir : groupDirs) {
            Future<Void> f = executionService.submit(ASYNC_EXECUTOR, new RestoreTask(groupDir, raftService));
            futures.add(f);
        }

        // TODO [basri] fix timeout
        waitWithDeadline(futures, 2, TimeUnit.MINUTES, RETHROW_EVERYTHING);

        this.startCompleted = true;
    }

    public void shutdown() {
        directoryLock.release();
    }

    public boolean isStartCompleted() {
        return startCompleted;
    }

    private File getGroupDir(RaftGroupId groupId) {
        return new File(dir, groupId.getName() + "@" + groupId.getSeed() + "@" + groupId.getId());
    }

    private RaftGroupId getGroupId(File groupDir) {
        String[] split = groupDir.getName().split("@");
        assert split.length == 3;

        return new RaftGroupId(split[0], Long.parseLong(split[1]), Long.parseLong(split[2]));
    }

    private File[] getGroupDirs() {
        return dir.listFiles(new FileFilter() {
            @Override
            public boolean accept(File path) {
                return path.isDirectory()
                        && new File(path, OnDiskRaftStateStore.MEMBERS_FILENAME).exists()
                        && new File(path, OnDiskRaftStateStore.TERM_FILENAME).exists();
            }
        });
    }

    private class RestoreTask implements Callable<Void> {
        private final File groupDir;
        private final RaftService raftService;

        RestoreTask(File groupDir, RaftService raftService) {
            this.groupDir = groupDir;
            this.raftService = raftService;
        }

        @Override
        public Void call() throws Exception {
            final RaftGroupId groupId = getGroupId(groupDir);
            logger.info("Restoring " + groupId + " from " + groupDir);
            OnDiskRaftStateLoader stateLoader =
                    new OnDiskRaftStateLoader(groupDir, uncommittedEntryCount, node.getSerializationService());
            final RestoredRaftState restoredState = stateLoader.load();

            if (!localMember.getUuid().equals(restoredState.localEndpoint().getUuid())) {
                 throw new IllegalStateException("Local member: " + localMember + ", endpoint: " + restoredState.localEndpoint()
                    + ", group: " + groupId);
            }

            boolean metadataGroup = groupId.getName().equals(METADATA_CP_GROUP_NAME);
            if (metadataGroup && groupId.getId() != 0L) {
                raftService.getMetadataGroupManager().setMetadataGroupId(groupId);
            }

            final RaftNodeImpl raftNode = raftService.restoreRaftNode(groupId, restoredState, stateLoader.logFileStructure());

            if (metadataGroup) {
                new Thread(createThreadName(node.hazelcastInstance.getName(), "cp-metadata-restore-thread")) {
                    public void run() {
                        publishCpMembersUntilLeaderElection(groupId, raftNode, restoredState);
                    }
                }.start();
            }
            logger.info("Completed restore of " + groupId);
            return null;
        }

        private void publishCpMembersUntilLeaderElection(RaftGroupId groupId, RaftNodeImpl raftNode,
                RestoredRaftState restoredState) {
            BiTuple<Long, List<CPMemberInfo>> tuple2 = updateInvocationManagerMembers(raftService, groupId, restoredState);
            long entryIndex = tuple2.element1;
            List<CPMemberInfo> cpMembers = tuple2.element2;

            // TODO: fixme timeout
            long timeout = TimeUnit.MINUTES.toMillis(2);
            long deadline = Clock.currentTimeMillis() + timeout;

            ClusterService clusterService = node.getClusterService();
            OperationService operationService = node.getNodeEngine().getOperationService();
            Operation op = new PublishRestoredCPMembersOp(groupId, entryIndex, cpMembers);

            while (Clock.currentTimeMillis() < deadline && raftNode.getLeader() == null) {
                if (logger.isFineEnabled()) {
                    logger.fine("Broadcasting restored CP members list...");
                }

                for (Member member : clusterService.getMembers(NON_LOCAL_MEMBER_SELECTOR)) {
                    operationService.send(op, member.getAddress());
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }

            if (raftNode.getLeader() == null) {
                logger.severe("Metadata CP group leader election could not not be completed in time during recovery...");
            }
        }

        private BiTuple<Long, List<CPMemberInfo>> updateInvocationManagerMembers(RaftService service, RaftGroupId groupId,
                RestoredRaftState restoredState) {
            long index = 0;
            List<CPMemberInfo> members = null;
            SnapshotEntry snapshot = restoredState.snapshot();
            if (snapshot != null) {
                RestoreSnapshotOp op = (RestoreSnapshotOp) snapshot.operation();
                MetadataRaftGroupSnapshot metadataSnapshot = (MetadataRaftGroupSnapshot) op.getSnapshot();
                index = metadataSnapshot.getMembersCommitIndex();
                members = new ArrayList<CPMemberInfo>(metadataSnapshot.getMembers());
                updateInvocationManagerMembers(service, groupId, index, members);
            }
            for (LogEntry entry : restoredState.entries()) {
                Object op = entry.operation();
                if (op instanceof InitMetadataRaftGroupOp) {
                    // Having multiple InitMetadataRaftGroupOp entries is allowed and expected.
                    // Every metadata member appends its own init operation during CP discovery.
                    index = entry.index();
                    members = ((InitMetadataRaftGroupOp) op).getDiscoveredCPMembers();
                    updateInvocationManagerMembers(service, groupId, entry.index(), members);
                }
                if (op instanceof AddCPMemberOp) {
                    assert members != null;
                    index = entry.index();
                    members.add(((AddCPMemberOp) op).getMember());
                    updateInvocationManagerMembers(service, groupId, entry.index(), members);
                }
                if (op instanceof RemoveCPMemberOp) {
                    assert members != null;
                    index = entry.index();
                    members.remove(((RemoveCPMemberOp) op).getMember());
                    updateInvocationManagerMembers(service, groupId, entry.index(), members);
                }
            }
            return BiTuple.of(index, members);
        }

        private void updateInvocationManagerMembers(RaftService service, RaftGroupId groupId,
                long index, List<CPMemberInfo> members) {
            service.updateInvocationManagerMembers(groupId.getSeed(), index, members);
            if (logger.isFineEnabled()) {
                logger.fine("Restored seed: " + groupId.getSeed() + ", members commit index: " + index
                        + ", CP member list: " + members);
            }
        }
    }
}
