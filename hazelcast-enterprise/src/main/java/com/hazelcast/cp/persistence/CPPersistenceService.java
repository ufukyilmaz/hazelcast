package com.hazelcast.cp.persistence;

import com.hazelcast.config.cp.RaftAlgorithmConfig;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.raft.impl.persistence.LogFileStructure;
import com.hazelcast.cp.internal.raft.impl.persistence.RaftStateStore;
import com.hazelcast.cp.internal.raft.impl.persistence.RestoredRaftState;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.spi.impl.executionservice.ExecutionService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cp.persistence.CPMemberMetadataStoreImpl.CP_METADATA_FILE_NAME;
import static com.hazelcast.internal.util.FutureUtil.RETHROW_EVERYTHING;
import static com.hazelcast.internal.util.FutureUtil.waitWithDeadline;
import static com.hazelcast.spi.impl.executionservice.ExecutionService.ASYNC_EXECUTOR;

public class CPPersistenceService {

    public static final String SERVICE_NAME = "hz:ee:cpPersistenceService";
    private static final String CP_DIRECTORY_NAME = "cp";

    private final Node node;
    private final File baseDir;
    private final CPMemberMetadataStoreImpl cpMemberMetadataStore;
    private volatile boolean startCompleted;
    private final int uncommittedEntryCount;

    public CPPersistenceService(Node node, File dir) {
        this.node = node;
        this.baseDir = new File(dir, CP_DIRECTORY_NAME);
        baseDir.mkdir();
        assert baseDir.exists();
        this.cpMemberMetadataStore = new CPMemberMetadataStoreImpl(baseDir);
        RaftAlgorithmConfig config = node.getConfig().getCPSubsystemConfig().getRaftAlgorithmConfig();
        this.uncommittedEntryCount = config.getUncommittedEntryCountToRejectNewAppends();
    }

    public static boolean isCPDirectory(File dir) {
        File cpDir = new File(dir, CP_DIRECTORY_NAME);
        return cpDir.exists() && new File(cpDir, CP_METADATA_FILE_NAME).exists();
    }

    public CPMemberMetadataStoreImpl getCpMemberMetadataStore() {
        return cpMemberMetadataStore;
    }

    @Nonnull
    public RaftStateStore createRaftStateStore(
            @Nonnull RaftGroupId groupId, @Nullable LogFileStructure logFileStructure
    ) {
        return new OnDiskRaftStateStore(
                getGroupDir(groupId), node.getSerializationService(), uncommittedEntryCount, logFileStructure);
    }

    public void restore() {
        File[] groupDirs = getGroupDirs();
        if (groupDirs == null || groupDirs.length == 0) {
            startCompleted = true;
            return;
        }

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

    public boolean isStartCompleted() {
        return startCompleted;
    }

    public FilenameFilter getFilenameFilter() {
        return new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.equals(CP_DIRECTORY_NAME);
            }
        };
    }

    private File getGroupDir(RaftGroupId groupId) {
        return new File(baseDir, groupId.name() + "@" + groupId.seed() + "@" + groupId.id());
    }

    private RaftGroupId getGroupId(File groupDir) {
        String[] split = groupDir.getName().split("@");
        assert split.length == 3;

        return new RaftGroupId(split[0], Long.parseLong(split[1]), Long.parseLong(split[2]));
    }

    private File[] getGroupDirs() {
        return baseDir.listFiles(new FileFilter() {
            @Override
            public boolean accept(File path) {
                return path.isDirectory() && new File(path, "members").exists() && new File(path, "term").exists();
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
            RaftGroupId groupId = getGroupId(groupDir);
            OnDiskRaftStateLoader stateLoader =
                    new OnDiskRaftStateLoader(groupDir, uncommittedEntryCount, node.getSerializationService());
            RestoredRaftState restoredState = stateLoader.load();
            raftService.restoreRaftNode(groupId, restoredState, stateLoader.logFileStructure());
            return null;
        }
    }
}
