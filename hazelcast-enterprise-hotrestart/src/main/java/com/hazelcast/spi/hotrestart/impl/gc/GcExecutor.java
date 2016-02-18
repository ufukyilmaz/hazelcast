package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.hotrestart.HotRestartKey;
import com.hazelcast.spi.hotrestart.impl.HotRestartStoreConfig;
import com.hazelcast.spi.hotrestart.impl.HotRestartStoreImpl.CatchupRunnable;
import com.hazelcast.spi.hotrestart.impl.HotRestartStoreImpl.CatchupTestSupport;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.ActiveChunk;
import com.hazelcast.util.collection.Long2LongHashMap;
import com.hazelcast.util.concurrent.BackoffIdleStrategy;
import com.hazelcast.util.concurrent.IdleStrategy;
import com.hazelcast.util.concurrent.OneToOneConcurrentArrayQueue;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.concurrent.locks.LockSupport;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.Thread.currentThread;
import static java.lang.Thread.interrupted;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

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
    /** This lock exists only to serve the needs of {@link #runWhileGcPaused(CatchupRunnable)}. */
    private final Object testGcMutex = new Object();
    private volatile boolean backpressure;
    private volatile Throwable gcThreadFailureCause;

    private boolean started;
    private boolean stopped;

    public GcExecutor(HotRestartStoreConfig cfg, GcHelper gcHelper) {
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
                boolean didWork = false;
                while (!stopped && !interrupted()) {
                    final int workCount;
                    synchronized (testGcMutex) {
                        workCount = mc.catchupNow();
                        final GcParams gcp = (workCount > 0 || didWork) ? chunkMgr.gcParams() : GcParams.ZERO;
                        if (gcp.forceGc) {
                            didWork = runForcedGC(gcp);
                        } else {
                            didWork = chunkMgr.valueGc(gcp, mc);
                        }
                        if (didWork) {
                            mc.catchupNow();
                            chunkMgr.tombGc(mc);
                        }
                        didWork |= pfixTombstoMgr.sweepAsNeeded();
                    }
                    if (didWork) {
                        Thread.yield();
                    }
                    if (workCount > 0 || didWork) {
                        idleCount = 0;
                    } else {
                        idler.idle(idleCount++);
                    }
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
        try {
            return chunkMgr.valueGc(gcp, mc);
        } finally {
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
        } catch (InterruptedException e) {
            currentThread().interrupt();
        }
    }

    public void submitRecord(HotRestartKey key, long freshSeq, int freshSize, boolean freshIsTombstone) {
        submit(chunkMgr.new AddRecord(key, freshSeq, freshSize, freshIsTombstone));
    }

    public void submitReplaceActiveChunk(final ActiveChunk closed, final ActiveChunk fresh) {
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
    public void runWhileGcPaused(CatchupRunnable task) {
        synchronized (testGcMutex) {
            task.run(mc);
        }
    }

    private static BackoffIdleStrategy idler() {
        return new BackoffIdleStrategy(SPIN_COUNT, YIELD_COUNT, 1, MILLISECONDS.toNanos(MAX_PARK_MILLIS));
    }

    /**
     * Instance of this class is passed around to allow catching up with
     * the mutator thread at any point along the GC cycle codepath.
     */
    public class MutatorCatchup implements CatchupTestSupport {
        private static final int LATE_CATCHUP_THRESHOLD_MILLIS = 10;
        private static final int LATE_CATCHUP_CUTOFF_MILLIS = 110;
        // counts the number of calls to catchupAsNeeded since last catchupNow
        private long i;
        private long lastCaughtUp;

        public int catchupAsNeeded() {
            return catchupAsNeeded(DEFAULT_CATCHUP_INTERVAL_LOG2);
        }

        int catchupAsNeeded(int power) {
            return (i++ & ((1 << power) - 1)) == 0 ? catchUpWithMutator() : 0;
        }

        @Override public int catchupNow() {
            i = 1;
            return catchUpWithMutator();
        }

        private int catchUpWithMutator() {
            final int workCount = workQueue.drainTo(workDrain, WORK_QUEUE_CAPACITY);
            if (logger.isFineEnabled()) {
                diagnoseLateCatchup();
            }
            if (workCount == 0) {
                return 0;
            }
            for (Runnable op : workDrain) {
                op.run();
            }
            workDrain.clear();
            return workCount;
        }

        private void diagnoseLateCatchup() {
            final long now = System.nanoTime();
            final long sinceLastCatchup = now - lastCaughtUp;
            lastCaughtUp = now;
            if (sinceLastCatchup > MILLISECONDS.toNanos(LATE_CATCHUP_THRESHOLD_MILLIS)
                && sinceLastCatchup < MILLISECONDS.toNanos(LATE_CATCHUP_CUTOFF_MILLIS)
            ) {
                final StringWriter sw = new StringWriter(512);
                final PrintWriter w = new PrintWriter(sw);
                new Exception().printStackTrace(w);
                final String trace = sw.toString();
                final Matcher m = Pattern.compile("\n.*?\n.*?\n.*?(\n.*?\n.*?)\n").matcher(trace);
                m.find();
                logger.fine("Didn't catch up for %d ms%s", NANOSECONDS.toMillis(sinceLastCatchup), m.group(1));
            }
        }

        public boolean shutdownRequested() {
            return stopped;
        }
    }
}
