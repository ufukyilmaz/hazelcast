package com.hazelcast.internal.hotrestart.impl.gc.tracker;

/**
 * Tracks the GC state for a single key handle in the Hot Restart store: which chunk holds
 * the live record, whether it's a tombstone, and the global garbage count for the key handle.
 * The tracker may also refer to a key handle with no live record, if its global garbage count
 * is still non-zero. This condition can occur when the record is interred by a prefix tombstone.
 * <p>
 * The global garbage count is the number of all garbage records with the given key handle still
 * present in value chunks, excluding those records which are known to be interred by a prefix tombstone.
 * This count reaching zero means that, if the currently live record is a tombstone, it can be declared
 * garbage since there are no more records left that it must protect from resurrection.
 */
public abstract class Tracker {

    /** @return whether there is a live record for this tracker's key handle */
    public final boolean isAlive() {
        return rawChunkSeq() != 0;
    }

    /** @return the chunk seq of the live record corresponding to this tracker */
    public final long chunkSeq() {
        return isTombstone() ? -rawChunkSeq() : rawChunkSeq();
    }

    /** @return whether the live record corresponding to this tracker is a tombstone */
    public final boolean isTombstone() {
        return rawChunkSeq() < 0;
    }

    /**
     * Makes this tracker point to a new chunk seq as the location of the live record.
     * @param destChunkSeq seq of the chunk holding the live record
     */
    public final void moveToChunk(long destChunkSeq) {
        setLiveState(destChunkSeq, isTombstone());
    }

    /**
     * Signals that a new live record with this tracker's key handle for has entered the system.
     *
     * @param chunkSeq seq of the chunk where the record was written
     * @param freshIsTombstone whether the new record is a tombstone
     * @param owner the {@link TrackerMap} owning this tracker (needed to notify its callback)
     * @param restarting whether this is called during Hot Restart, used only for an assertion
     */
    public final void newLiveRecord(long chunkSeq, boolean freshIsTombstone, TrackerMap owner, boolean restarting) {
        final TrackerMapBase ownr = (TrackerMapBase) owner;
        if (isAlive()) {
            final boolean staleIsTombstone = isTombstone();
            if (staleIsTombstone) {
                if (!freshIsTombstone) {
                    ownr.replacedTombstoneWithValue();
                } else {
                    assert restarting : "Attempted to replace a tombstone with another tombstone";
                }
            } else {
                incrementGarbageCount();
                if (freshIsTombstone) {
                    ownr.replacedValueWithTombstone();
                }
            }
        } else {
            ownr.added(freshIsTombstone);
        }
        setLiveState(chunkSeq, freshIsTombstone);
    }

    /**
     * Signals that the live record that this tracker points to was retired.
     * @param owner the {@link TrackerMap} owning this tracker (needed to notify its callback)
     */
    public final void retire(TrackerMap owner) {
        ((TrackerMapBase) owner).retired(isTombstone());
        setRawChunkSeq(0);
    }

    /** Increments the global garbage count for this tracker. */
    public final void incrementGarbageCount() {
        setGarbageCount(garbageCount() + 1);
    }

    /** Reduces the global garbage count for this tracker by the specified amount. */
    public final boolean reduceGarbageCount(int amount) {
        setGarbageCount(garbageCount() - amount);
        assert garbageCount() >= 0 : String.format(
                "Global garbage count went below zero after decrementing by %,d:  %,d", amount, garbageCount());
        return garbageCount() == 0;
    }

    /** Resets this tracker's global garbage count to zero. */
    final void resetGarbageCount() {
        setGarbageCount(0);
    }

    /**
     * Updates the state of this tracker to refer to another live record.
     * @param chunkSeq seq of the chunk where the live record is written
     * @param isTombstone whether the record is a tombstone
     */
    final void setLiveState(long chunkSeq, boolean isTombstone) {
        assert chunkSeq != 0 : "Attempt to move a record to chunk zero";
        setRawChunkSeq(isTombstone ? -chunkSeq : chunkSeq);
    }

    /** @return this tracker's global garbage count */
    public abstract long garbageCount();

    /** Sets this tracker's global garbage count to the given value. */
    abstract void setGarbageCount(long garbageCount);

    /** @return this tracker's raw "chunkSeq" value. Negative values represent the chunk seq of a tombstone,
     * positive values represent the chunk seq of a value record, and zero represents a dead record. */
    abstract long rawChunkSeq();

    /** Sets this tracker's raw "chunkSeq" value. */
    abstract void setRawChunkSeq(long rawChunkSeqValue);
}
