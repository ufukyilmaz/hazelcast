package com.hazelcast.spi.hotrestart.impl.gc;

/**
 * Represents a tombstone chunk whose on-disk contents are stable (immutable).
 */
class StableTombChunk extends StableChunk {

    StableTombChunk(WriteThroughTombChunk from, boolean compressed) {
        super(from, compressed);
    }

    StableTombChunk(long seq, RecordMap records, int liveRecordCount, long size, long garbage) {
        super(seq, records, liveRecordCount, size, garbage, false, false);
    }

    @Override String base() {
        return TOMB_BASEDIR;
    }
}
