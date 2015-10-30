package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.hotrestart.HotRestartKey;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.util.collection.Long2LongHashMap;
import com.hazelcast.util.concurrent.BackoffIdleStrategy;
import com.hazelcast.util.concurrent.IdleStrategy;
import com.hazelcast.util.concurrent.OneToOneConcurrentArrayQueue;

import java.util.ArrayList;
import java.util.concurrent.locks.LockSupport;

import static java.lang.Thread.currentThread;
import static java.lang.Thread.interrupted;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Top-level control code of the GC thread. Only thread mechanics are here;
 * actual GC logic is in {@link ChunkManager}.
 */
public final class GcExecutor {
    /** Capacity of the work queue which is used by the mutator thread to submit
     * tasks to the G thread. */
    @SuppressWarnings("checkstyle:magicnumber")
    public static final int WORK_QUEUE_CAPACITY = 1 << 10;
    /** How many times to busy-spin while waiting for another thread. */
    public static final int SPIN_COUNT = 1000;
    /** How many times to yield while waiting for another thread. */
    public static final int YIELD_COUNT = 100;
    /** Max parkNanos time while waiting for another thread. */
    public static final long MAX_PARK_MILLIS = 1;
    /** How many times to parkNanos in the main loop before using the
     * idle time to compress some chunk files. */
    public static final int PARK_COUNT_BEFORE_COMPRESS = 50;
    /** Base-2 log of the number of calls to {@link MutatorCatchup#catchupAsNeeded()}
     * before deciding to catch up. */
    public static final int DEFAULT_CATCHUP_INTERVAL_LOG2 = 10;

    /** The chunk manager. Referenced from {@code HotRestartStoreBase#hotRestart()}. */
    public final ChunkManager chunkMgr;
    private final PrefixTombstoneManager pfixTombstoMgr;
    private final Runnable shutdown = new Runnable() {
        @Override public void run() {
            keepGoing = false;
        }
    };
    private final OneToOneConcurrentArrayQueue<Runnable> workQueue =
            new OneToOneConcurrentArrayQueue<Runnable>(WORK_QUEUE_CAPACITY);
    private final ArrayList<Runnable> workDrain = new ArrayList<Runnable>(WORK_QUEUE_CAPACITY);
    private final Thread gcThread;
    private final MutatorCatchup mc = new MutatorCatchup();
    private final IdleStrategy mutatorIdler = idler();
    private final GcLogger logger;
    private final GcHelper gcHelper;
    private volatile boolean backpressure;
    private volatile Throwable gcThreadFailureCause;
    private boolean keepGoing;

    public GcExecutor(GcHelper gcHelper, String name) {
        this.gcHelper = gcHelper;
        this.logger = gcHelper.logger;
        this.gcThread = new Thread(new MainLoop(), "GC thread for " + name);
        this.pfixTombstoMgr = new PrefixTombstoneManager(this, logger);
        this.chunkMgr = new ChunkManager(gcHelper, pfixTombstoMgr);
        pfixTombstoMgr.setChunkManager(chunkMgr);
    }

    public void setPrefixTombstones(Long2LongHashMap prefixTombstones) {
        pfixTombstoMgr.setPrefixTombstones(prefixTombstones);
    }

    private class MainLoop implements Runnable {
        @SuppressWarnings("checkstyle:npathcomplexity")
        @Override public void run() {
            final IdleStrategy idler = idler();
            try {
                int parkCount = 0;
                boolean didWork = false;
                while (keepGoing && !interrupted()) {
                    final int workCount = Math.max(0, mc.catchupNow());
                    final GcParams gcp = (workCount != 0 || didWork) ? chunkMgr.gcParams() : GcParams.ZERO;
                    if (gcp.forceGc) {
                        didWork = runForcedGC(gcp);
                    } else {
                        didWork = chunkMgr.gc(gcp, mc);
                    }
                    didWork |= pfixTombstoMgr.sweepAsNeeded();
                    if (idler.idle(workCount + (didWork ? 1 : 0))) {
                        parkCount++;
                    } else {
                        parkCount = 0;
                    }
                    if (gcHelper.compressionEnabled() && parkCount >= PARK_COUNT_BEFORE_COMPRESS) {
                        didWork = chunkMgr.compressSomeChunk(mc);
                        parkCount = 0;
                    }
                }
                // This should be optional, configurable behavior.
                // Compression is performed while the rest of the system
                // is already down, thus contributing to downtime.
                if (gcHelper.compressionEnabled()) {
                    chunkMgr.compressAllChunks(mc);
                }
                logger.info("GC thread done. ");
            } catch (Throwable t) {
                logger.severe("GC thread terminated by exception", t);
                gcThreadFailureCause = t;
                keepGoing = false;
            } finally {
                chunkMgr.close();
            }
        }
    }

    boolean runForcedGC(GcParams gcp) {
        backpressure = true;
        final boolean savedFsyncOften = mc.fsyncOften;
        mc.fsyncOften = false;
        try {
            return chunkMgr.gc(gcp, mc);
        } finally {
            mc.fsyncOften = savedFsyncOften;
            backpressure = false;
        }
    }

    public void start() {
        keepGoing = true;
        chunkMgr.gcHelper.prepareGcThread(gcThread);
        gcThread.start();
    }

    public void shutdown() {
        if (!gcThread.isAlive()) {
            return;
        }
        try {
            submit(shutdown);
            while (gcThread.isAlive()) {
                LockSupport.unpark(gcThread);
                Thread.sleep(1);
            }
            chunkMgr.gcHelper.dispose();
        } catch (InterruptedException e) {
            currentThread().interrupt();
        }
    }

    public void submitRecord(HotRestartKey key, long freshSeq, int freshSize, boolean freshIsTombstone) {
        submit(chunkMgr.new AddRecord(key, freshSeq, freshSize, freshIsTombstone));
    }

    public void submitReplaceActiveChunk(final WriteThroughChunk closed, final WriteThroughChunk fresh) {
        submit(chunkMgr.new ReplaceActiveChunk(fresh, closed));
    }

    public void addPrefixTombstones(long[] prefixes) {
        pfixTombstoMgr.addPrefixTombstones(prefixes);
    }

    void submit(Runnable task) {
        if (!keepGoing) {
            throw new HotRestartException("keepGoing == false", gcThreadFailureCause);
        }
        boolean submitted = false;
//        boolean reportedBlocking = false;
        while (!(submitted || (submitted = workQueue.offer(task))) || backpressure) {
            if (mutatorIdler.idle(0)) {
//                if (!reportedBlocking) {
//                    System.out.println(submitted? "Backpressure" : "Blocking to submit");
//                    reportedBlocking = true;
//                }
                if (!gcThread.isAlive()) {
                    throw new HotRestartException("GC thread has died", gcThreadFailureCause);
                }
            }
        }
        // work has been done (task submitted), so reset idler state
        mutatorIdler.idle(1);
    }

    private static BackoffIdleStrategy idler() {
        return new BackoffIdleStrategy(SPIN_COUNT, YIELD_COUNT, 1, MILLISECONDS.toNanos(MAX_PARK_MILLIS));
    }

    /**
     * Instance of this class is passed around to allow catching up with
     * the mutator thread at any point along the GC cycle codepath.
     */
    class MutatorCatchup {
        // Consulted by output streams to decide whether to fsync after each buffer flush.
        // Perhaps expose this as configuration param (currently it's hardcoded).
        boolean fsyncOften;
        // counts the number of calls to catchupAsNeeded since last catch up
        private long i;

        int catchupAsNeeded() {
            return catchupAsNeeded(DEFAULT_CATCHUP_INTERVAL_LOG2);
        }

        int catchupAsNeeded(int power) {
            return (i++ & ((1 << power) - 1)) == 0 ? catchUpWithMutator() : 0;
        }

        int catchupNow() {
            i = 0;
            return catchupAsNeeded(0);
        }

        @SuppressWarnings("checkstyle:innerassignment")
        private int catchUpWithMutator() {
            final int workCount;
            if ((workCount = workQueue.drainTo(workDrain, WORK_QUEUE_CAPACITY)) == 0) {
                return 0;
            }
            for (Runnable op : workDrain) {
                op.run();
            }
            workDrain.clear();
            return workCount;
        }

        void dismissGarbage(Chunk c) {
            chunkMgr.dismissGarbage(c);
        }

        void dismissGarbageRecord(Chunk c, KeyHandle kh, GcRecord r) {
            chunkMgr.dismissGarbageRecord(c, kh, r);
        }

        boolean shutdownRequested() {
            return !keepGoing;
        }
    }
}
