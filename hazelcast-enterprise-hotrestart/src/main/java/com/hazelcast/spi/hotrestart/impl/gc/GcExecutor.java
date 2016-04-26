package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.spi.hotrestart.HotRestartKey;
import com.hazelcast.spi.hotrestart.impl.ConcurrentConveyorSingleQueue;
import com.hazelcast.spi.hotrestart.impl.HotRestartPersistenceEngine.CatchupRunnable;
import com.hazelcast.spi.hotrestart.impl.di.Inject;
import com.hazelcast.spi.hotrestart.impl.di.Name;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.ActiveChunk;

public final class GcExecutor {
    /** Capacity of the work queue which is used by the mutator thread to submit
     * tasks to the GC thread. */
    @SuppressWarnings("checkstyle:magicnumber")
    public static final int WORK_QUEUE_CAPACITY = 1 << 10;

    private final ChunkManager chunkMgr;
    private final Object testGcMutex;
    private final ConcurrentConveyorSingleQueue<Runnable> conveyor;
    private final Thread gcThread;
    private final MutatorCatchup mc;

    @Inject
    private GcExecutor(@Name("gcConveyor") ConcurrentConveyorSingleQueue<Runnable> gcConveyor, ChunkManager chunkMgr,
                       MutatorCatchup mc, GcMainLoop mainLoop, @Name("storeName") String storeName,
                       @Name("testGcMutex") Object testGcMutex
    ) {
        this.chunkMgr = chunkMgr;
        this.gcThread = new Thread(mainLoop, "GC thread for " + storeName);
        this.conveyor = gcConveyor;
        this.mc = mc;
        this.testGcMutex = testGcMutex;
    }

    public void start() {
        gcThread.start();
    }

    public void shutdown() {
        conveyor.submit(new Runnable() {
            @Override public void run() {
                mc.askedToStop = true;
            }
        });
        conveyor.awaitDrainerGone();
    }

    public void submitRecord(HotRestartKey key, long freshSeq, int freshSize, boolean freshIsTombstone) {
        conveyor.submit(chunkMgr.new AddRecord(key, freshSeq, freshSize, freshIsTombstone));
    }

    public void submitReplaceActiveChunk(final ActiveChunk closed, final ActiveChunk fresh) {
        conveyor.submit(chunkMgr.new ReplaceActiveChunk(fresh, closed));
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

}
