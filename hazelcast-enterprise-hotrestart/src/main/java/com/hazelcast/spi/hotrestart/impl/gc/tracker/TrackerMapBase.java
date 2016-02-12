package com.hazelcast.spi.hotrestart.impl.gc.tracker;

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.internal.util.counters.SwCounter;

import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;

/**
 * Abstract base class for methods common to both tracker map implementations.
 */
public abstract class TrackerMapBase implements TrackerMap {

    @Probe(level = MANDATORY) public final SwCounter liveValues = newSwCounter();
    @Probe(level = MANDATORY) public final SwCounter liveTombstones = newSwCounter();

    @Override public void removeLiveTombstone(KeyHandle kh) {
        liveTombstones.inc(-1);
        doRemove(kh);
    }

    @Override public void removeIfDead(KeyHandle kh, Tracker tr) {
        if (!tr.isAlive()) {
            doRemove(kh);
        }
    }

    abstract void doRemove(KeyHandle kh);

    final void added(boolean isTombstone) {
        (isTombstone ? liveTombstones : liveValues).inc();
    }

    final void retired(boolean isTombstone) {
        (isTombstone ? liveTombstones : liveValues).inc(-1);
    }

    final void replacedTombstoneWithValue() {
        liveTombstones.inc(-1);
        liveValues.inc();
    }

    final void replacedValueWithTombstone() {
        liveValues.inc(-1);
        liveTombstones.inc();
    }

    @Override public String toString() {
        final StringBuilder b = new StringBuilder(1024);
        for (Cursor c = cursor(); c.advance();) {
            b.append(c.asKeyHandle()).append("->").append(c.asTracker()).append(' ');
        }
        return b.toString();
    }
}
