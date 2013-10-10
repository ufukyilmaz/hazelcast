package com.hazelcast.elasticmemory;

import com.hazelcast.elasticmemory.util.MemoryUnit;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.storage.Storage;

import java.io.Closeable;
import java.util.logging.Level;

import static com.hazelcast.elasticmemory.util.MathUtil.divideByAndCeil;

class ByteBufferStorage implements Storage<DataRefImpl> {

    private static final int MIN_SEGMENT_COUNT = 2;
    private static final int MAX_SEGMENT_SIZE_IN_MB = 1024;

    private final ILogger logger = Logger.getLogger(getClass().getName());
    private final int segmentCount;
    private final BufferSegment[] segments;

    public ByteBufferStorage(int totalSizeInMb, int chunkSizeInKb) {
        if (totalSizeInMb <= 0) {
            throw new IllegalArgumentException("Total size must be positive!");
        }
        if (chunkSizeInKb <= 0) {
            throw new IllegalArgumentException("Chunk size must be positive!");
        }
        if (totalSizeInMb >= MAX_SEGMENT_SIZE_IN_MB * MIN_SEGMENT_COUNT) {
            segmentCount = divideByAndCeil(totalSizeInMb, MAX_SEGMENT_SIZE_IN_MB);
        } else {
            segmentCount = totalSizeInMb >= MIN_SEGMENT_COUNT ? MIN_SEGMENT_COUNT : 1;
        }
        boolean sizeAdjusted = false;
        if (totalSizeInMb % segmentCount != 0) {
            totalSizeInMb = divideByAndCeil(totalSizeInMb, segmentCount) * segmentCount;
            if (totalSizeInMb == 0) {
                totalSizeInMb = MIN_SEGMENT_COUNT;
            }
            sizeAdjusted = true;
        }

        int segmentSizeInKb = (int) MemoryUnit.MEGABYTES.toKiloBytes(totalSizeInMb / segmentCount);
        if (segmentSizeInKb % chunkSizeInKb != 0) {
            segmentSizeInKb = divideByAndCeil(segmentSizeInKb, chunkSizeInKb) * chunkSizeInKb;
            totalSizeInMb = ((int) MemoryUnit.KILOBYTES.toMegaBytes(segmentSizeInKb)) * segmentCount;
            sizeAdjusted = true;
        }

        if (sizeAdjusted) {
            logger.info("Adjusting totalSize to: " + totalSizeInMb + "MB.");
        }

        if (logger.isFinestEnabled()) {
            logger.info(segmentCount + " off-heap segments (each " + segmentSizeInKb + "KB) will be created.");
        } else {
            logger.info(segmentCount + " off-heap segments (each " + totalSizeInMb / segmentCount + "MB) will be created.");
        }
        this.segments = new BufferSegment[segmentCount];
        for (int i = 0; i < segmentCount; i++) {
            segments[i] = new BufferSegment(segmentSizeInKb, chunkSizeInKb);
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
            final Closeable resource;
            if ((resource = resources[i]) != null) {
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
