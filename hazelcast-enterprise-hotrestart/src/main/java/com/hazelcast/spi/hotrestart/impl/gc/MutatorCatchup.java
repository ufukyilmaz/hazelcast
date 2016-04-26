package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.spi.hotrestart.impl.ConcurrentConveyor;
import com.hazelcast.spi.hotrestart.impl.HotRestartPersistenceEngine.CatchupTestSupport;
import com.hazelcast.spi.hotrestart.impl.di.Inject;
import com.hazelcast.spi.hotrestart.impl.di.Name;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Instance of this class is passed around to allow catching up with
 * the mutator thread at any point along the GC cycle codepath.
 */
public final class MutatorCatchup implements CatchupTestSupport {
    /** Base-2 log of the number of calls to {@link #catchupAsNeeded()} before deciding to catch up. */
    public static final int DEFAULT_CATCHUP_INTERVAL_LOG2 = 10;
    private static final int LATE_CATCHUP_THRESHOLD_MILLIS = 10;
    private static final int LATE_CATCHUP_CUTOFF_MILLIS = 110;

    boolean askedToStop;

    private final ConcurrentConveyor<Runnable> conveyor;
    private final ArrayList<Runnable> workDrain = new ArrayList<Runnable>(GcExecutor.WORK_QUEUE_CAPACITY);
    private final GcLogger logger;

    // counts the number of calls to catchupAsNeeded since last catchupNow
    private long i;
    private long lastCaughtUp = -1;
    // private long lastCaughtUp = logger.isFineEnabled() ? 0 : -1;

    @Inject
    private MutatorCatchup(GcLogger logger, @Name("gcConveyor") ConcurrentConveyor<Runnable> conveyor) {
        this.logger = logger;
        this.conveyor = conveyor;
    }

    public int catchupAsNeeded() {
        return catchupAsNeeded(DEFAULT_CATCHUP_INTERVAL_LOG2);
    }

    int catchupAsNeeded(int power) {
        return (i++ & ((1 << power) - 1)) == 0 ? catchUpWithMutator() : 0;
    }

    @Override
    public int catchupNow() {
        i = 1;
        return catchUpWithMutator();
    }

    public boolean shutdownRequested() {
        return askedToStop;
    }

    private int catchUpWithMutator() {
        final int workCount = conveyor.drainTo(workDrain);
        if (lastCaughtUp >= 0) {
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
}
