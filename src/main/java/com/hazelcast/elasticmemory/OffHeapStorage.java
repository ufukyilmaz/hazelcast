package com.hazelcast.elasticmemory;


import com.hazelcast.nio.serialization.Data;
import com.hazelcast.storage.Storage;

import java.util.logging.Level;

import static com.hazelcast.elasticmemory.util.MathUtil.divideByAndCeil;


public class OffHeapStorage extends OffHeapStorageSupport implements Storage<DataRefImpl> {

    private final BufferSegment[] segments;

    public OffHeapStorage(int totalSizeInMb, int chunkSizeInKb) {
        this(totalSizeInMb, divideByAndCeil(totalSizeInMb, MAX_SEGMENT_SIZE_IN_MB), chunkSizeInKb);
    }

    public OffHeapStorage(int totalSizeInMb, int segmentCount, int chunkSizeInKb) {
        super(totalSizeInMb, segmentCount, chunkSizeInKb);
        logger.log(Level.INFO, "Total of " + segmentCount + " segments is going to be initialized...");
        this.segments = new BufferSegment[segmentCount];
        for (int i = 0; i < segmentCount; i++) {
            segments[i] = new BufferSegment(segmentSizeInMb, chunkSizeInKb);
        }
    }

    private BufferSegment getSegment(int hash) {
        return segments[(hash == Integer.MIN_VALUE) ? 0 : Math.abs(hash) % segmentCount];
    }

    public DataRefImpl put(int hash, Data value) {
        return getSegment(hash).put(value);
    }

    public Data get(int hash, DataRefImpl ref) {
        return getSegment(hash).get(ref);
    }

    public void remove(int hash, DataRefImpl ref) {
        getSegment(hash).remove(ref);
    }

    public void destroy() {
        destroy(segments);
    }
}
