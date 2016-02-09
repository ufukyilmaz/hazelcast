package com.hazelcast.spi.hotrestart.impl.gc.chunk;

import com.hazelcast.nio.Disposable;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.impl.gc.record.Record;
import com.hazelcast.spi.hotrestart.impl.gc.record.RecordMap;

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

    /** Value chunk file size limit in bytes. */
    @SuppressWarnings("checkstyle:magicnumber")
    public static final long VAL_SIZE_LIMIT = 8 << 20;

    /** Tombstone chunk file size limit in bytes. */
    @SuppressWarnings("checkstyle:magicnumber")
    public static final long TOMB_SIZE_LIMIT = 1 << 20;

    /** Name of the base directory for value records. */
    public static final String VAL_BASEDIR = "value";

    /** Name of the base directory for tombstone records. */
    public static final String TOMB_BASEDIR = "tombstone";

    /** Unique sequence number of this chunk. */
    public final long seq;

    public final RecordMap records;
    public long garbage;
    public int liveRecordCount;
    /** Will be true when a new prefix tombstone arrives and this chunk
     * may contain records interred by it. */
    private boolean needsDismissing;

    public Chunk(long seq, RecordMap records) {
        this.seq = seq;
        this.records = records;
    }

    public Chunk(GrowingChunk from) {
        this.seq = from.seq;
        this.records = from.records;
        this.liveRecordCount = from.liveRecordCount;
        this.garbage = from.garbage;
        this.needsDismissing = from.needsDismissing();
    }

    public final boolean needsDismissing() {
        return needsDismissing;
    }

    public void needsDismissing(boolean needsDismissing) {
        this.needsDismissing = needsDismissing;
    }

    public Chunk(long seq, RecordMap records, int liveRecordCount, long garbage) {
        this.seq = seq;
        this.records = records;
        this.liveRecordCount = liveRecordCount;
        this.garbage = garbage;
    }

    public abstract long size();

    public boolean compressed() {
        return false;
    }

    public void retire(KeyHandle kh, Record r, boolean mayIncrementGarbageCount) {
        assert records.get(kh).liveSeq() == r.liveSeq()
                : String.format("%s.retire(%s, %s) but have %s", this, kh, r, records.get(kh));
        garbage += r.size();
        r.retire(mayIncrementGarbageCount);
        liveRecordCount--;
    }

    public void retire(KeyHandle kh, Record r) {
        retire(kh, r, true);
    }

    public String fnameSuffix() {
        return FNAME_SUFFIX;
    }

    public String base() {
        return VAL_BASEDIR;
    }

    public void dispose() {
        records.dispose();
    }

    @Override public String toString() {
        return String.format("%s(%03x,%,d,%,d)",
                getClass().getSimpleName(), seq, liveRecordCount, garbage);
    }
}
