package com.hazelcast.internal.hotrestart.impl.gc.chunk;

import com.hazelcast.internal.util.collection.LongSet;
import com.hazelcast.internal.hotrestart.impl.RestartItem;
import com.hazelcast.internal.hotrestart.impl.gc.record.RecordMap;

import static com.hazelcast.internal.hotrestart.impl.gc.chunk.EmptyLongSet.emptyLongSet;

/**
 * Represents a chunk whose on-disk contents are stable (immutable).
 */
public final class StableValChunk extends StableChunk {

    /**
     * A key prefix could exist in a chunk file, yet during Hot Restart no record with that key prefix
     * might be added to a record map due to a prefix tombstone
     * ({@link com.hazelcast.internal.hotrestart.impl.gc.Rebuilder#acceptCleared(RestartItem)} doesn't add
     * the record to the record map). Then the prefix tombstone might be incorrectly garbage-collected,
     * leading to the resurrection of dead records. To prevent that, this set holds the key prefixes of all
     * records in this chunk interred by a prefix tombstone and is consulted in
     * {@link com.hazelcast.internal.hotrestart.impl.gc.PrefixTombstoneManager.Sweeper#markLiveTombstones(Chunk)}.
     * <p>
     * This set will be empty for all chunks except those that were constructed by {@code Rebuilder} during
     * Hot Restart.
     */
    public final LongSet clearedPrefixesFoundAtRestart;

    public StableValChunk(ActiveValChunk from) {
        super(from);
        this.clearedPrefixesFoundAtRestart = emptyLongSet();
    }

    public StableValChunk(
            long seq, RecordMap records, int liveRecordCount, long size, long garbage, boolean needsDismissing
    ) {
        this(seq, records, emptyLongSet(), liveRecordCount, size, garbage, needsDismissing);
    }

    public StableValChunk(long seq, RecordMap records, LongSet clearedPrefixesFoundAtRestart, int liveRecordCount,
                          long size, long garbage, boolean needsDismissing) {
        super(seq, records, liveRecordCount, size, garbage, needsDismissing);
        this.clearedPrefixesFoundAtRestart = clearedPrefixesFoundAtRestart.isEmpty()
                ? emptyLongSet() : clearedPrefixesFoundAtRestart;
    }

    /** @return the GC cost of this chunk (amount of live data in it). */
    public long cost() {
        return size() - garbage;
    }

    /** Updates the cached value of the benefit/cost factor.
     * @return the updated value. */
    public double updateBenefitToCost(long currChunkSeq) {
        return benefitToCost = benefitToCost(currChunkSeq);
    }

    private double benefitToCost(long currChunkSeq) {
        final double benefit = this.garbage;
        final double cost = cost();
        if (cost == 0) {
            return Double.POSITIVE_INFINITY;
        }
        final double age = currChunkSeq - this.seq;
        return age * benefit / cost;
    }
}
