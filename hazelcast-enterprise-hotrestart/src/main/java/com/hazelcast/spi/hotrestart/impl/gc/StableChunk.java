package com.hazelcast.spi.hotrestart.impl.gc;

import static com.hazelcast.spi.hotrestart.impl.gc.Compressor.COMPRESSED_SUFFIX;

/**
 * Represents a tombstone chunk whose on-disk contents are stable (immutable).
 */
class StableChunk extends Chunk {
    boolean compressed;
    private final long size;

    StableChunk(GrowingChunk from, boolean compressed) {
        super(from);
        this.size = from.size();
        this.compressed = compressed;
        this.needsDismissing = from.needsDismissing;
    }

    StableChunk(long seq, RecordMap records, int liveRecordCount,
                long size, long garbage, boolean needsDismissing, boolean compressed
    ) {
        super(seq, records, liveRecordCount, garbage);
        this.size = size;
        this.compressed = compressed;
        this.needsDismissing = needsDismissing;
    }

    @Override final long size() {
        return size;
    }

    @Override final String fnameSuffix() {
        return Chunk.FNAME_SUFFIX + (compressed ? COMPRESSED_SUFFIX : "");
    }
}
