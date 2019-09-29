package com.hazelcast.internal.hotrestart.impl.gc;

import com.hazelcast.internal.util.concurrent.ConcurrentConveyor;
import com.hazelcast.internal.hotrestart.impl.di.Inject;
import com.hazelcast.internal.hotrestart.impl.di.Name;

import static com.hazelcast.internal.util.concurrent.ConcurrentConveyor.SUBMIT_IDLER;
import static java.lang.Thread.interrupted;

/**
 * {@code Runnable} that runs the main loop of the GC thread.
 */
public final class GcMainLoop implements Runnable {
    private final ChunkManager chunkMgr;
    private final MutatorCatchup mc;
    private final PrefixTombstoneManager pfixTombstoMgr;
    private final ConcurrentConveyor<Runnable> gcConveyor;
    private final ConcurrentConveyor<?> persistenceConveyor;
    private final GcLogger logger;
    private final Object testGcMutex;

    @Inject
    private GcMainLoop(ChunkManager chunkMgr, MutatorCatchup mc, PrefixTombstoneManager pfixTombstoMgr,
                       @Name("persistenceConveyor") ConcurrentConveyor<?> persistenceConveyor,
                       @Name("gcConveyor") ConcurrentConveyor<Runnable> gcConveyor, GcLogger logger,
                       @Name("testGcMutex") Object testGcMutex
    ) {
        this.chunkMgr = chunkMgr;
        this.mc = mc;
        this.pfixTombstoMgr = pfixTombstoMgr;
        this.persistenceConveyor = persistenceConveyor;
        this.gcConveyor = gcConveyor;
        this.logger = logger;
        this.testGcMutex = testGcMutex;
    }

    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity"})
    @Override
    public void run() {
        try {
            long idleCount = 0;
            boolean didWork = false;
            while (!mc.askedToStop && !interrupted()) {
                final int workCount;
                synchronized (testGcMutex) {
                    workCount = mc.catchupNow();
                    if (!pfixTombstoMgr.sweepingInProgress()) {
                        final GcParams gcp = (workCount > 0 || didWork) ? chunkMgr.gcParams() : GcParams.ZERO;
                        didWork = gcp.forceGc ? runForcedGC(gcp) : chunkMgr.valueGc(gcp, mc);
                        if (didWork) {
                            mc.catchupNow();
                            chunkMgr.tombGc(mc);
                        }
                    }
                    didWork |= pfixTombstoMgr.sweepAsNeeded();
                }
                if (didWork) {
                    Thread.yield();
                }
                if (workCount > 0 || didWork) {
                    idleCount = 0;
                } else {
                    SUBMIT_IDLER.idle(idleCount++);
                }
            }
            gcConveyor.drainerDone();
            logger.fine("GC thread done. ");
        } catch (Throwable t) {
            gcConveyor.drainerFailed(t);
            logger.severe("GC thread terminated by exception", t);
        } finally {
            mc.askedToStop = true;
        }
    }

    private boolean runForcedGC(GcParams gcp) {
        persistenceConveyor.backpressureOn();
        try {
            return chunkMgr.valueGc(gcp, mc);
        } finally {
            persistenceConveyor.backpressureOff();
        }
    }
}
