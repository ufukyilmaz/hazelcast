package com.hazelcast.internal.hotrestart.impl.gc;

import com.hazelcast.internal.util.concurrent.ConcurrentConveyor;
import com.hazelcast.internal.hotrestart.impl.di.Inject;
import com.hazelcast.internal.hotrestart.impl.di.Name;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.hazelcast.internal.hotrestart.impl.gc.GcExecutor.COLLECTOR_QUEUE_CAPACITY;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Instance of this class is passed around to allow catching up with the mutator thread
 * at any point along the GC cycle codepath.
 */
// Class non-final for the sake of Mockito
@SuppressWarnings("checkstyle:finalclass")
public class MutatorCatchup {
    /** Base-2 log of the number of calls to {@link #catchupAsNeeded()} before deciding to catch up. */
    public static final int DEFAULT_CATCHUP_INTERVAL_LOG2 = 10;
    private static final int LATE_CATCHUP_THRESHOLD_MILLIS = 10;
    private static final int LATE_CATCHUP_CUTOFF_MILLIS = 300;

    boolean askedToStop;

    private final ConcurrentConveyor<Runnable> conveyor;
    private final Snapshotter snapshotter;
    private final ArrayList<Runnable> workDrain = new ArrayList<Runnable>(COLLECTOR_QUEUE_CAPACITY);
    private final GcLogger logger;

    // counts the number of calls to catchupAsNeeded since last catchupNow
    private long i;
    // when initialized to a negative value, "late catchup diagnostics" are disabled
    private long lastCaughtUp = -1;

    @Inject
    private MutatorCatchup(
            GcLogger logger, @Name("gcConveyor") ConcurrentConveyor<Runnable> conveyor, Snapshotter snapshotter
    ) {
        this.logger = logger;
        this.conveyor = conveyor;
        this.snapshotter = snapshotter;
    }

    /**
     * Calls {@link #catchupAsNeeded(int)} with the value of {@link #DEFAULT_CATCHUP_INTERVAL_LOG2}.
     */
    // method non-final for the sake of Mockito
    public int catchupAsNeeded() {
        return catchupAsNeeded(DEFAULT_CATCHUP_INTERVAL_LOG2);
    }

    /**
     * Catches up "as needed", every 2<sup>power</sup> calls to this method.
     * The call count is reset if {@link #catchupNow()} is called.
     * @param power as explained above
     * @return the number of work items drained from the work queue and processed
     */
    final int catchupAsNeeded(int power) {
        return (i++ & ((1 << power) - 1)) == 0 ? catchUpWithMutator() : 0;
    }

    /**
     * Caches up with the mutator thread.
     * @return the number of work items drained from the work queue and processed
     */
    // method non-final for the sake of Mockito
    public int catchupNow() {
        i = 1;
        return catchUpWithMutator();
    }

    /** @return whether the GC thread was asked to stop. */
    public boolean shutdownRequested() {
        return askedToStop;
    }

    private int catchUpWithMutator() {
        final int workCount = conveyor.drainTo(workDrain);
        if (lastCaughtUp >= 0) {
            lastCaughtUp = diagnoseLateCatchup("Collector", lastCaughtUp, logger);
        }
        if (snapshotter.enabled) {
            snapshotter.takeChunkSnapshotAsNeeded();
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

    public static long diagnoseLateCatchup(String name, long lastCaughtUp, GcLogger logger) {
        final long now = System.nanoTime();
        final long sinceLastCatchup = now - lastCaughtUp;
        if (sinceLastCatchup <= MILLISECONDS.toNanos(LATE_CATCHUP_THRESHOLD_MILLIS)
            || sinceLastCatchup >= MILLISECONDS.toNanos(LATE_CATCHUP_CUTOFF_MILLIS)
        ) {
            return now;
        }
        final StringWriter sw = new StringWriter(512);
        final PrintWriter w = new PrintWriter(sw);
        new Exception().printStackTrace(w);
        final String trace = sw.toString();
        final Matcher m = Pattern.compile("\n.*?\n.*?\n.*?(\n.*?\n.*?)\n").matcher(trace);
        m.find();
        logger.finest("%s didn't catch up for %d ms%s", name, NANOSECONDS.toMillis(sinceLastCatchup), m.group(1));
        return now;
    }


    /** Allows a task to be run with access to the {@link MutatorCatchup}. Exclusively for testing purposes. */
    public interface CatchupRunnable {
        void run(MutatorCatchup mc);
    }
}
