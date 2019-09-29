package com.hazelcast.internal.hotrestart.impl.gc;

import com.hazelcast.internal.util.concurrent.ConcurrentConveyorSingleQueue;
import com.hazelcast.internal.hotrestart.HotRestartKey;
import com.hazelcast.internal.hotrestart.impl.di.Inject;
import com.hazelcast.internal.hotrestart.impl.di.Name;
import com.hazelcast.internal.hotrestart.impl.gc.MutatorCatchup.CatchupRunnable;
import com.hazelcast.internal.hotrestart.impl.gc.chunk.ActiveChunk;

import java.io.File;

/** Contains top-level control code for the GC thread. */
// class non-final for mockability
public class GcExecutor {
    /** Capacity of the work queue which is used by the mutator thread to submit tasks to the GC thread. */
    @SuppressWarnings("checkstyle:magicnumber")
    public static final int COLLECTOR_QUEUE_CAPACITY = 1 << 10;

    private final ChunkManager chunkMgr;
    private final Object testGcMutex;
    private final ConcurrentConveyorSingleQueue<Runnable> conveyor;
    private final Thread gcThread;
    private final MutatorCatchup mc;

    @Inject
    GcExecutor(@Name("gcConveyor") ConcurrentConveyorSingleQueue<Runnable> gcConveyor, ChunkManager chunkMgr,
                       MutatorCatchup mc, GcMainLoop mainLoop, @Name("storeName") String storeName,
                       @Name("testGcMutex") Object testGcMutex
    ) {
        this.chunkMgr = chunkMgr;
        this.gcThread = new Thread(mainLoop, storeName + ".GC-thread");
        this.conveyor = gcConveyor;
        this.mc = mc;
        this.testGcMutex = testGcMutex;
    }

    /** Starts the GC thread. */
    public void start() {
        gcThread.start();
    }

    /** Asks the GC thread to stop and awaits its completion. */
    public void shutdown() {
        conveyor.submit(new Runnable() {
            @Override public void run() {
                mc.askedToStop = true;
            }
        });
        conveyor.awaitDrainerGone();
    }

    /** Submits a "record added" event to the GC thread's work queue. */
    public void submitRecord(HotRestartKey key, long freshSeq, int freshSize, boolean freshIsTombstone) {
        conveyor.submit(chunkMgr.new AddRecord(key, freshSeq, freshSize, freshIsTombstone));
    }

    /** Submits a "active chunk replaced" event to the GC thread's work queue. */
    public void submitReplaceActiveChunk(final ActiveChunk closed, final ActiveChunk fresh) {
        conveyor.submit(chunkMgr.new ReplaceActiveChunk(fresh, closed));
    }

    /**
     * Runs the task while holding a mutex lock which is also held during GC activity.
     * Provided only to facilitate testing.
     */
    public void runWhileGcPaused(CatchupRunnable task) {
        synchronized (testGcMutex) {
            task.run(mc);
        }
    }

    public void submitBackup(File targetDir) {
        conveyor.submit(chunkMgr.new BackupChunks(targetDir));
    }
}
