/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.spi.hotrestart.impl.HotRestartStoreImpl;
import com.hazelcast.util.concurrent.BackoffIdleStrategy;
import com.hazelcast.util.concurrent.IdleStrategy;
import com.hazelcast.util.concurrent.OneToOneConcurrentArrayQueue;
import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.hotrestart.impl.gc.ChunkManager.GcParams;

import java.util.ArrayList;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.spi.hotrestart.impl.HotRestartStoreImpl.compression;
import static java.lang.Thread.currentThread;
import static java.lang.Thread.interrupted;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Top-level control code of the GC thread. Only thread mechanics are here;
 * actual GC logic is in {@link ChunkManager}.
 */
public class GcExecutor {
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

    /** The chunk manager. Referenced from {@link HotRestartStoreImpl#hotRestart()}. */
    public final ChunkManager chunkMgr;
    private final Task shutdown = new Task() {
        @Override public void perform() {
            keepGoing = false;
        }
    };
    private final OneToOneConcurrentArrayQueue<Task> workQueue =
            new OneToOneConcurrentArrayQueue<Task>(WORK_QUEUE_CAPACITY);
    private final ArrayList<Task> workDrain = new ArrayList<Task>(WORK_QUEUE_CAPACITY);
    private volatile boolean backpressure;
    private volatile Throwable gcThreadFailureCause;
    private final Thread gcThread = new Thread(new MainLoop(), "FastRestart GC");
    private final MutatorCatchup mc = new MutatorCatchup();
    private final IdleStrategy mutatorIdler = idler();
    private boolean keepGoing;

    public GcExecutor(ChunkManager chunkMgr) {
        this.chunkMgr = chunkMgr;
    }

    private class MainLoop implements Runnable {
        @SuppressWarnings("checkstyle:npathcomplexity")
        @Override public void run() {
            final IdleStrategy idler = idler();
            try {
                int parkCount = 0;
                boolean didWork = false;
                while (keepGoing && !interrupted()) {
                    final int workCount = mc.catchupNow();
                    final GcParams gcp = (workCount != 0 || didWork) ? chunkMgr.gParams() : GcParams.ZERO;
                    if (gcp.forceGc) {
                        backpressure = true;
                        didWork = chunkMgr.gc(gcp, mc);
                        backpressure = false;
                    } else {
                        didWork = chunkMgr.gc(gcp, mc);
                    }
                    if (idler.idle(workCount + (didWork ? 1 : 0))) {
                        parkCount++;
                    } else {
                        parkCount = 0;
                    }
                    if (compression && parkCount >= PARK_COUNT_BEFORE_COMPRESS) {
                        didWork = chunkMgr.compressSomeChunk(mc);
                        parkCount = 0;
                    }
                }
                if (compression) {
                    chunkMgr.compressAllChunks(mc);
                }
                System.err.println("GC thread done. ");
            } catch (Throwable t) {
                System.err.println("GC thread terminated by exception");
                t.printStackTrace();
                gcThreadFailureCause = t;
            }
        }
    }

    public void start() {
        keepGoing = true;
        gcThread.start();
    }

    public void shutdown() {
        try {
            submit(shutdown);
            while (gcThread.isAlive()) {
                LockSupport.unpark(gcThread);
                Thread.sleep(1);
            }
        } catch (InterruptedException e) {
            currentThread().interrupt();
        }
    }

    public void submitReplaceRecord(final Record stale, final Record fresh) {
        submit(chunkMgr.new ReplaceRecord(stale, fresh));
    }

    public void submitReplaceActiveChunk(final WriteThroughChunk closed, final WriteThroughChunk fresh) {
        submit(chunkMgr.new ReplaceActiveChunk(fresh, closed));
    }

    private void submit(Task task) {
        boolean reportedQueueFull = false;
        while (backpressure || !workQueue.offer(task)) {
            if (!reportedQueueFull) {
                System.out.println("Blocking to submit");
                reportedQueueFull = true;
            }
            if (mutatorIdler.idle(0) && !gcThread.isAlive()) {
                throw new HotRestartException("GC thread has died", gcThreadFailureCause);
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
        // Never set programmatically at the moment; perhaps expose as configuration param.
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
            int workCount;
            if (backpressure || (workCount = workQueue.drainTo(workDrain, WORK_QUEUE_CAPACITY)) == 0) {
                return 0;
            }
            for (Task op : workDrain) {
                op.perform();
            }
            workDrain.clear();
            return workCount;
        }

        void dismissGarbage(Chunk c) {
            chunkMgr.dismissGarbage(c);
        }

        void dismissGarbageRecord(Chunk c, Record r) {
            chunkMgr.dismissGarbageRecord(c, r);
        }
    }

    /** A task submitted to the GC thread's work queue. */
    interface Task {
        void perform();
    }
}
