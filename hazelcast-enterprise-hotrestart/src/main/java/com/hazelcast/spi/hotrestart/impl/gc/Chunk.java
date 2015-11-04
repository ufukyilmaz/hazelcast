package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.nio.Disposable;
import com.hazelcast.spi.hotrestart.KeyHandle;

/**
 * Represents a chunk file.
 */
public abstract class Chunk implements Disposable {
    /** Chunk filename suffix. */
    public static final String FNAME_SUFFIX = ".chunk";

    /** Suffix added to the chunk file while it is active. On restart this file
     * is the only one whose last entry may be incomplete. If the system failed while
     * it was being written out, the caller did not receive a successful response,
     * therefore that entry doesn't actually exist. */
    public static final String ACTIVE_CHUNK_SUFFIX = ".active";

    /** Suffix added to a chunk file while it is being written to during a GC cycle.
     * If system fails during GC, such file should not be considered during restart. */
    public static final String DEST_FNAME_SUFFIX = Chunk.FNAME_SUFFIX + ".dest";

    /** Chunk file size limit. */
    @SuppressWarnings("checkstyle:magicnumber")
    public static final long SIZE_LIMIT = 8 << 20;

    /** Unique sequence number of this chunk. */
    public final long seq;

    final RecordMap records;
    long garbage;
    int liveRecordCount;
    /** Will be true when a new prefix tombstone arrives and this chunk
     * may contain records interred by it. */
    boolean needsDismissing;

    Chunk(long seq, RecordMap records) {
        this.seq = seq;
        this.records = records;
    }

    Chunk(WriteThroughChunk from) {
        this.seq = from.seq;
        this.records = from.records;
        this.liveRecordCount = from.liveRecordCount;
        this.garbage = from.garbage;
        this.needsDismissing = from.needsDismissing;
    }

    Chunk(long seq, RecordMap records, int liveRecordCount, long garbage) {
        this.seq = seq;
        this.records = records;
        this.liveRecordCount = liveRecordCount;
        this.garbage = garbage;
    }

    abstract long size();

    void retire(KeyHandle kh, Record r, boolean incrementGarbageCount) {
        assert records.get(kh).liveSeq() == r.liveSeq()
                : String.format("%s.retire(%s, %s) but have %s", this, kh, r, records.get(kh));
        garbage += r.size();
        r.retire(incrementGarbageCount);
        liveRecordCount--;
    }

    void retire(KeyHandle kh, Record r) {
        retire(kh, r, true);
    }

    String fnameSuffix() {
        return FNAME_SUFFIX;
    }

    public void dispose() {
        records.dispose();
    }

    @Override public String toString() {
        return String.format("%s(%03x,%,d,%,d)",
                getClass().getSimpleName(), seq, liveRecordCount, garbage);
    }
}
