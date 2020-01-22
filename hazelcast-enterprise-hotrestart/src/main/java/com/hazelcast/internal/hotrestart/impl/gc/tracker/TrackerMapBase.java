package com.hazelcast.internal.hotrestart.impl.gc.tracker;

import com.hazelcast.internal.hotrestart.KeyHandle;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.util.counters.SwCounter;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.HOTRESTART_METRIC_GC_LIVE_TOMBSTONES;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.HOTRESTART_METRIC_GC_LIVE_VALUES;
import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;

/**
 * Abstract base class common to both tracker map implementations. Manages the {@code liveValues}
 * and {@code liveTombstones} metrics.
 */
public abstract class TrackerMapBase implements TrackerMap {
    /**
     * The global count of live values in the store. Increases as records are added, decreases as they are retired or replaced
     * with tombstones
     */
    @Probe(name = HOTRESTART_METRIC_GC_LIVE_VALUES, level = MANDATORY)
    public final SwCounter liveValues = newSwCounter();
    /**
     * The global count of live tombstones in the store. Increases as records are added, decreases as they are retired. Can also
     * be decreased if there are no other records with the same key handle that it protects from resurrection.
     */
    @Probe(name = HOTRESTART_METRIC_GC_LIVE_TOMBSTONES, level = MANDATORY)
    public final SwCounter liveTombstones = newSwCounter();

    @Override
    public void removeLiveTombstone(KeyHandle kh) {
        liveTombstones.inc(-1);
        doRemove(kh);
    }

    @Override
    public void removeIfDead(KeyHandle kh, Tracker tr) {
        if (!tr.isAlive()) {
            doRemove(kh);
        }
    }

    abstract void doRemove(KeyHandle kh);

    /**
     * Callback that signals a record was added to this Hot Restart Store, which does not replace an
     * existing record.
     * @param isTombstone whether the record is a tombstone
     */
    final void added(boolean isTombstone) {
        (isTombstone ? liveTombstones : liveValues).inc();
    }

    /**
     * Callback that signals a record was retired.
     * @param isTombstone whether the record is a tombstone
     */
    final void retired(boolean isTombstone) {
        (isTombstone ? liveTombstones : liveValues).inc(-1);
    }

    /**
     * Callback that signals a tombstone was replaced with a value record.
     */
    final void replacedTombstoneWithValue() {
        liveTombstones.inc(-1);
        liveValues.inc();
    }

    /**
     * Callback that signals a value record was replaced with a tombstone.
     */
    final void replacedValueWithTombstone() {
        liveValues.inc(-1);
        liveTombstones.inc();
    }

    @Override
    public String toString() {
        final StringBuilder b = new StringBuilder(1024);
        for (Cursor c = cursor(); c.advance();) {
            b.append(c.asKeyHandle()).append("->").append(c.asTracker()).append(' ');
        }
        return b.toString();
    }
}
