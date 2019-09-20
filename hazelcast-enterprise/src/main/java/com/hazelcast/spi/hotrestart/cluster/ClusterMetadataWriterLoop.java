package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.cluster.Member;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.partition.PartitionTableView;
import com.hazelcast.internal.util.concurrent.MPSCQueue;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.version.Version;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.util.ThreadUtil.createThreadName;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Handles persisting cluster metadata such as, member list, partition table etc.
 * using a single persistence thread.
 * <p/>
 * Using single thread, writer loop persists metadata in submit order.
 * Good thing is, it can avoid persisting interim values when it detects a more recent value.
 */
final class ClusterMetadataWriterLoop implements Runnable {

    private static final long SLOW_PERSISTENCE_THRESHOLD_NANOS = SECONDS.toNanos(1);

    private final ILogger logger;
    private final MPSCQueue<Runnable> taskQueue = new MPSCQueue<>(null);
    private final MemberListHandler memberListHandler;
    private final PartitionTableHandler partitionTableHandler;
    private final ClusterStateHandler clusterStateHandler;
    private final ClusterVersionHandler clusterVersionHandler;
    private final Thread thread;

    private boolean stop;

    ClusterMetadataWriterLoop(File homeDir, Node node) {
        mkdir(homeDir);

        logger = node.getLogger(getClass());
        memberListHandler = new MemberListHandler(new MemberListWriter(homeDir, node));
        partitionTableHandler = new PartitionTableHandler(new PartitionTableWriter(homeDir));
        clusterStateHandler = new ClusterStateHandler(new ClusterStateWriter(homeDir));
        clusterVersionHandler = new ClusterVersionHandler(new ClusterVersionWriter(homeDir));

        thread = new Thread(this,
                createThreadName(node.hazelcastInstance.getName(), "cluster-metadata-persistence-thread"));
        taskQueue.setConsumerThread(thread);
    }

    private static void mkdir(File homeDir) {
        if (!homeDir.exists() && !homeDir.mkdirs()) {
            throw new HotRestartException("Cannot create Hot Restart cluster metadata directory: " + homeDir.getAbsolutePath());
        }
    }

    @Override
    public void run() {
        while (!stop) {
            try {
                Runnable task = taskQueue.take();
                task.run();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.severe(e);
                break;
            } catch (Throwable e) {
                logger.severe(e);
            }
        }
    }

    void start() {
        if (logger.isFineEnabled()) {
            logger.fine("Starting metadata writer thread.");
        }
        thread.start();
    }

    void stop(boolean awaitPendingTasks) {
        if (!awaitPendingTasks) {
            taskQueue.clear();
        }
        taskQueue.offer(new StopTask());
        try {
            thread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new HotRestartException("Interrupted while waiting for the metadata persistence thread to stop", e);
        }
    }

    void writeMembers(Collection<Member> members) {
        memberListHandler.set(members);
        taskQueue.offer(memberListHandler);
    }

    void writePartitionTable(PartitionTableView partitionTable) {
        partitionTableHandler.set(partitionTable);
        taskQueue.offer(partitionTableHandler);
    }

    void writeClusterState(ClusterState state) {
        clusterStateHandler.set(state);
        taskQueue.offer(clusterStateHandler);
    }

    void writeClusterVersion(Version clusterVersion) {
        clusterVersionHandler.set(clusterVersion);
        taskQueue.offer(clusterVersionHandler);
    }

    private final class StopTask implements Runnable {
        @Override
        public void run() {
            if (logger.isFineEnabled()) {
                logger.fine("Stopping metadata writer thread.");
            }
            stop = true;
        }
    }

    private abstract class StatefulTask<T> implements Runnable {
        private final AtomicReference<T> state = new AtomicReference<>();

        final void set(T value) {
            state.set(value);
        }

        final T getAndReset() {
            return state.getAndSet(null);
        }
    }

    private final class ClusterStateHandler extends StatefulTask<ClusterState> {
        private final ClusterStateWriter writer;

        private ClusterStateHandler(ClusterStateWriter writer) {
            this.writer = writer;
        }

        @Override
        public void run() {
            ClusterState state = getAndReset();
            if (state == null) {
                return;
            }
            if (logger.isFineEnabled()) {
                logger.fine("Persisting cluster state: " + state);
            }
            persist(writer, state, "Cluster State");
        }
    }

    private final class ClusterVersionHandler extends StatefulTask<Version> {
        private final ClusterVersionWriter writer;

        private ClusterVersionHandler(ClusterVersionWriter writer) {
            this.writer = writer;
        }

        @Override
        public void run() {
            Version version = getAndReset();
            if (version == null) {
                return;
            }
            if (logger.isFineEnabled()) {
                logger.fine("Persisting cluster version: " + version);
            }
            persist(writer, version, "Cluster Version");
        }
    }

    private final class PartitionTableHandler extends StatefulTask<PartitionTableView> {
        private final AbstractMetadataWriter<PartitionTableView> writer;

        private PartitionTableHandler(AbstractMetadataWriter<PartitionTableView> writer) {
            this.writer = writer;
        }

        @Override
        public void run() {
            PartitionTableView partitionTable = getAndReset();
            if (partitionTable == null) {
                return;
            }
            if (logger.isFinestEnabled()) {
                logger.finest("Persisting partition table version: " + partitionTable.getVersion());
            }
            persist(writer, partitionTable, "Partition Table");
        }
    }

    private final class MemberListHandler extends StatefulTask<Collection<Member>> {
        private final MemberListWriter writer;

        private MemberListHandler(MemberListWriter writer) {
            this.writer = writer;
        }

        @Override
        public void run() {
            Collection<Member> members = getAndReset();
            if (members == null) {
                return;
            }
            if (logger.isFineEnabled()) {
                logger.fine("Persisting " + members.size() + " (active & passive) members -> " + members);
            }
            persist(writer, members, "Member List");
        }
    }

    @SuppressWarnings("checkstyle:illegaltype")
    private <T> void persist(AbstractMetadataWriter<T> writer, T value, String type) {
        try {
            long startTimeNanos = System.nanoTime();
            writer.write(value);

            long durationNanos = System.nanoTime() - startTimeNanos;
            if (durationNanos > SLOW_PERSISTENCE_THRESHOLD_NANOS) {
                long durationMillis = NANOSECONDS.toMillis(durationNanos);
                logger.warning("Slow disk IO! " + type + " persistence took " + durationMillis + " ms.");
            }
        } catch (IOException e) {
            logger.severe("While persisting " + type, e);
        }
    }
}
