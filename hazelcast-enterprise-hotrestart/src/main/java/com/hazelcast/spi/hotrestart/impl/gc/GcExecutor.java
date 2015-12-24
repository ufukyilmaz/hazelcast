package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.hotrestart.HotRestartKey;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.impl.HotRestartStoreConfig;
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
            stopped = true;
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

    private boolean started;
    private boolean stopped;
    /** This lock exists only to serve the needs of {@link #runWhileGcPaused(Runnable)}. */
    private final Object gcMutex = new Object();

    public GcExecutor(HotRestartStoreConfig cfg, GcHelper gcHelper) {
        this.gcHelper = gcHelper;
        this.logger = gcHelper.logger;
        this.gcThread = new Thread(new MainLoop(), "GC thread for " + cfg.storeName());
        this.pfixTombstoMgr = new PrefixTombstoneManager(this, logger);
        this.chunkMgr = new ChunkManager(cfg, gcHelper, pfixTombstoMgr);
        pfixTombstoMgr.setChunkManager(chunkMgr);
    }

    public void setPrefixTombstones(Long2LongHashMap prefixTombstones) {
        pfixTombstoMgr.setPrefixTombstones(prefixTombstones);
    }

    private class MainLoop implements Runnable {
        @SuppressWarnings({ "checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity" })
        @Override public void run() {
            final IdleStrategy idler = idler();
            try {
                long idleCount = 0;
                int parkCount = 0;
                boolean didWork = false;
                while (!stopped && !interrupted()) {
                    final int workCount = mc.catchupNow();
                    final GcParams gcp = (workCount > 0 || didWork) ? chunkMgr.gcParams() : GcParams.ZERO;
                    synchronized (gcMutex) {
                        if (gcp.forceGc) {
                            didWork = runForcedGC(gcp);
                        } else {
                            didWork = chunkMgr.gc(gcp, mc);
                        }
                        didWork |= pfixTombstoMgr.sweepAsNeeded();
                        didWork |= chunkMgr.deleteGarbageTombChunks(mc);
                    }
                    if (didWork) {
                        Thread.yield();
                    }
                    if (workCount > 0 || didWork) {
                        idleCount = 0;
                        parkCount = 0;
                    } else if (idler.idle(idleCount++)) {
                        parkCount++;
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
                stopped = true;
                logger.severe("GC thread terminated by exception", t);
                gcThreadFailureCause = t;
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
        started = true;
        gcThread.start();
    }

    public void shutdown() {
        if (!stopped && gcThread.isAlive()) {
            submit(shutdown);
        }
        try {
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
        if (stopped) {
            throw new HotRestartException("stopped == true", gcThreadFailureCause);
        }
        boolean submitted = false;
//        boolean reportedBlocking = false;
        for (long i = 0; !(submitted || (submitted = workQueue.offer(task))) || backpressure; i++) {
            if (mutatorIdler.idle(i)) {
//                if (!reportedBlocking) {
//                    System.out.println(submitted? "Backpressure" : "Blocking to submit");
//                    reportedBlocking = true;
//                }
                if (!gcThread.isAlive()) {
                    if (!started && submitted || task == shutdown) {
                        return;
                    }
                    throw new HotRestartException(
                            "GC thread died before task could be submitted", gcThreadFailureCause);
                }
            }
        }
    }

    /**
     * Runs the task while holding a mutex lock which is also held during GC activity.
     * This method is provided only to facilitate testing.
     */
    public void runWhileGcPaused(Runnable task) {
        synchronized (gcMutex) {
            task.run();
        }
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
        // counts the number of calls to catchupAsNeeded since last catchupNow
        private long i;

        int catchupAsNeeded() {
            return catchupAsNeeded(DEFAULT_CATCHUP_INTERVAL_LOG2);
        }

        int catchupAsNeeded(int power) {
            return (i++ & ((1 << power) - 1)) == 0 ? catchUpWithMutator() : 0;
        }

        int catchupNow() {
            i = 1;
            return catchUpWithMutator();
        }

        private int catchUpWithMutator() {
            final int workCount = workQueue.drainTo(workDrain, WORK_QUEUE_CAPACITY);
            if (workCount == 0) {
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
            return stopped;
        }
    }
}
