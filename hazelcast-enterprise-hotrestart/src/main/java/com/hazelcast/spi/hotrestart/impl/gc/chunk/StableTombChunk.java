package com.hazelcast.spi.hotrestart.impl.gc.chunk;

import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.impl.gc.record.Record;
import com.hazelcast.spi.hotrestart.impl.gc.record.RecordMap;
import com.hazelcast.spi.hotrestart.impl.gc.record.RecordMap.Cursor;
import com.hazelcast.util.collection.Long2ObjectHashMap;

/**
 * Represents a tombstone chunk whose on-disk contents are stable (immutable).
 */
public final class StableTombChunk extends StableChunk {

    private double benefitToCost;

    private Long2ObjectHashMap<KeyHandle> seqToKeyHandle;

    StableTombChunk(WriteThroughTombChunk from, boolean compressed) {
        super(from, compressed);
    }

    public StableTombChunk(long seq, RecordMap records, int liveRecordCount, long size, long garbage) {
        super(seq, records, liveRecordCount, size, garbage, false, false);
    }

    @Override public String base() {
        return TOMB_BASEDIR;
    }

    @Override public void retire(KeyHandle kh, Record r, boolean mayIncrementGarbageCount) {
        if (seqToKeyHandle != null) {
            seqToKeyHandle.remove(r.liveSeq());
        }
        super.retire(kh, r, mayIncrementGarbageCount);
    }

    public void initLiveSeqToKeyHandle() {
        seqToKeyHandle = new Long2ObjectHashMap<KeyHandle>(liveRecordCount, 0L);
        for (Cursor cursor = records.cursor(); cursor.advance();) {
            final Record r = cursor.asRecord();
            if (r.isAlive()) {
                seqToKeyHandle.put(r.liveSeq(), cursor.toKeyHandle());
            }
        }
    }

    public KeyHandle getLiveKeyHandle(long seq) {
        return seqToKeyHandle.get(seq);
    }

    public double cachedBenefitToCost() {
        return benefitToCost;
    }

    public double updateBenefitToCost() {
        return benefitToCost = benefitToCost(garbage, size());
    }

    @SuppressWarnings("checkstyle:magicnumber")
    public static double benefitToCost(long garbage, long size) {
        // Benefit is the amount of garbage, cost is the sum of read cost and write cost.
        // We assume a weighted read cost of size/2 and a write cost of size - garbage (i.e., live data size).
        // benefitToCost = benefit/cost = garbage / (size/2 + size - garbage) = garbage / (3/2 * size - garbage)
        // To simplify, we define g := garbage / size. Then, benefitToCost = g / (3/2 - g).
        final double g = (double) garbage / size;
        return g / (1.5 - g);
    }
}
