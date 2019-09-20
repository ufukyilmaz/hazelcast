package com.hazelcast.spi.hotrestart.impl.gc.chunk;

import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.impl.gc.record.Record;
import com.hazelcast.spi.hotrestart.impl.gc.record.RecordMap;
import com.hazelcast.spi.hotrestart.impl.gc.record.RecordMap.Cursor;
import com.hazelcast.internal.util.collection.Long2ObjectHashMap;

import java.util.Arrays;

/**
 * Stable tombstone chunk.
 */
public final class StableTombChunk extends StableChunk {

    private Long2ObjectHashMap<KeyHandle> filePosToKeyHandle;

    StableTombChunk(WriteThroughTombChunk from) {
        super(from);
    }

    public StableTombChunk(long seq, RecordMap records, int liveRecordCount, long size, long garbage) {
        super(seq, records, liveRecordCount, size, garbage, false);
    }

    @Override
    public String base() {
        return TOMB_BASEDIR;
    }

    @Override
    public void retire(KeyHandle kh, Record r, boolean mayIncrementGarbageCount) {
        if (filePosToKeyHandle != null) {
            filePosToKeyHandle.remove(r.filePosition());
        }
        super.retire(kh, r, mayIncrementGarbageCount);
    }

    @Override
    public void needsDismissing(boolean needsDismissing) {
        // A tombstone chunk never needs dismissing. Ignore the request to raise the flag.
    }

    /**
     * Initializes the file position-to-key handle map used during TombGC to look up a tombstone
     * at a given file position, in order to determine its liveness status.
     * @return array of the file positions of all currently live tombstones, in ascending order.
     */
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
        assert i == liveRecordCount : String.format("found %d live records, but liveRecordCount == %d", i, liveRecordCount);
        Arrays.sort(filePositions);
        return filePositions;
    }

    /**
     * @param filePos a file position in this tombstone chunk
     * @return the key handle of the tombstone at the supplied position, if that tombstone is still alive
     */
    public KeyHandle getLiveKeyHandle(long filePos) {
        return filePosToKeyHandle.get(filePos);
    }

    /** Disposes the file position-to-key handle map. */
    public void disposeFilePosToKeyHandle() {
        filePosToKeyHandle = null;
    }

    /**
     * Updates the cached value of the Benefit-Cost factor.
     * @return the value just calculated
     */
    public double updateBenefitToCost() {
        return benefitToCost = benefitToCost(garbage, size());
    }

    /**
     * Calculates the Benefit-Cost factor.
     * @param garbage amount of garbage bytes in a chunk
     * @param size total bytes in a chunk
     */
    @SuppressWarnings("checkstyle:magicnumber")
    public static double benefitToCost(long garbage, long size) {
        if (size == 0) {
            return Double.POSITIVE_INFINITY;
        }
        // Benefit is the amount of garbage to reclaim.
        // Cost is the I/O cost of copying live data (proportional to the amount of live data).
        // Benefit-to-cost the ratio of benefit to cost.
        final double g = (double) garbage / size;
        return g / (1 - g);
    }
}
