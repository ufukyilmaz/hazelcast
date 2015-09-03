package com.hazelcast.elasticmemory;

import com.hazelcast.internal.storage.Storage;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.nio.serialization.Data;

import java.io.Closeable;
import java.util.logging.Level;

import static com.hazelcast.util.QuickMath.divideByAndCeilToInt;
import static com.hazelcast.util.QuickMath.divideByAndCeilToLong;

@SuppressWarnings("unused")
class ByteBufferStorage implements Storage<DataRefImpl> {

    private static final int MIN_SEGMENT_COUNT = 2;
    private static final long MAX_SEGMENT_SIZE = MemoryUnit.GIGABYTES.toBytes(1);

    private final ILogger logger = Logger.getLogger(getClass().getName());
    private final int segmentCount;
    private final BufferSegment[] segments;

    public ByteBufferStorage(long capacity, int chunkSize) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be positive!");
        }
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("Chunk size must be positive!");
        }
        if (capacity >= MAX_SEGMENT_SIZE * MIN_SEGMENT_COUNT) {
            segmentCount = divideByAndCeilToInt(capacity, (int) MAX_SEGMENT_SIZE);
        } else {
            segmentCount = 1;
        }
        boolean sizeAdjusted = false;
        if (capacity % segmentCount != 0) {
            capacity = divideByAndCeilToLong(capacity, segmentCount) * segmentCount;
            if (capacity == 0) {
                capacity = MIN_SEGMENT_COUNT;
            }
            sizeAdjusted = true;
        }

        long segmentCapacity = capacity / segmentCount;
        if (segmentCapacity % chunkSize != 0) {
            segmentCapacity = (long) divideByAndCeilToInt(segmentCapacity, chunkSize) * chunkSize;
            capacity = segmentCapacity * segmentCount;
            sizeAdjusted = true;
        }

        if (sizeAdjusted) {
            logger.info("Adjusting capacity to: " + capacity + " bytes...");
        }

        logger.info(segmentCount + " off-heap segments (each " + segmentCapacity + " bytes) will be created.");
        this.segments = new BufferSegment[segmentCount];
        for (int i = 0; i < segmentCount; i++) {
            segments[i] = new BufferSegment((int) segmentCapacity, chunkSize);
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

    public int getSegmentCount() {
        return segmentCount;
    }

    protected final void destroy(final Closeable... resources) {
        for (int i = 0; i < resources.length; i++) {
            Closeable resource = resources[i];
            if (resource != null) {
                try {
                    resources[i] = null;
                    resource.close();
                } catch (Throwable e) {
                    logger.log(Level.WARNING, e.getMessage(), e);
                }
            }
        }
        System.gc();
    }
}
