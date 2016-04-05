package com.hazelcast.spi.hotrestart.impl.gc.chunk;

import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.impl.gc.record.Record;
import com.hazelcast.spi.hotrestart.impl.gc.record.RecordMap;
import com.hazelcast.spi.hotrestart.impl.gc.record.RecordMap.Cursor;
import com.hazelcast.util.collection.Long2ObjectHashMap;

import java.util.Arrays;

/**
 * Represents a tombstone chunk whose on-disk contents are stable (immutable).
 */
public final class StableTombChunk extends StableChunk {

    private double benefitToCost;

    private Long2ObjectHashMap<KeyHandle> filePosToKeyHandle;

    StableTombChunk(WriteThroughTombChunk from, boolean compressed) {
        super(from);
    }

    public StableTombChunk(long seq, RecordMap records, int liveRecordCount, long size, long garbage) {
        super(seq, records, liveRecordCount, size, garbage, false);
    }

    @Override public String base() {
        return TOMB_BASEDIR;
    }

    @Override public void retire(KeyHandle kh, Record r, boolean mayIncrementGarbageCount) {
        if (filePosToKeyHandle != null) {
            filePosToKeyHandle.remove(r.filePosition());
        }
        super.retire(kh, r, mayIncrementGarbageCount);
    }

    @Override public void needsDismissing(boolean needsDismissing) {
        // A tombstone chunk never needs dismissing. Ignore the request to raise the flag.
    }

    public int[] initFilePosToKeyHandle() {
        final int[] filePositions = new int[liveRecordCount];
        filePosToKeyHandle = new Long2ObjectHashMap<KeyHandle>(liveRecordCount);
        int i = 0;
        for (Cursor cursor = records.cursor(); cursor.advance();) {
            final Record r = cursor.asRecord();
            if (r.isAlive()) {
                filePosToKeyHandle.put(r.filePosition(), cursor.toKeyHandle());
                filePositions[i++] = r.filePosition();
            }
        }
        assert i == liveRecordCount;
        Arrays.sort(filePositions);
        return filePositions;
    }

    public KeyHandle getLiveKeyHandle(long filePos) {
        return filePosToKeyHandle.get(filePos);
    }

    public void disposeFilePosToKeyHandle() {
        filePosToKeyHandle = null;
    }

    public double cachedBenefitToCost() {
        return benefitToCost;
    }

    public double updateBenefitToCost() {
        return benefitToCost = benefitToCost(garbage, size());
    }

    @SuppressWarnings("checkstyle:magicnumber")
    public static double benefitToCost(long garbage, long size) {
        // Benefit is the amount of garbage to reclaim.
        // Cost is the I/O cost of copying live data (proportional to the amount of live data).
        // Benefit-to-cost the ratio of benefit to cost.
        final double g = (double) garbage / size;
        return g / (1 - g);
    }
}
