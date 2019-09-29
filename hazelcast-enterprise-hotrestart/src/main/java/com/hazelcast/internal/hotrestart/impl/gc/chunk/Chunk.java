package com.hazelcast.internal.hotrestart.impl.gc.chunk;

import com.hazelcast.internal.nio.Disposable;
import com.hazelcast.internal.hotrestart.KeyHandle;
import com.hazelcast.internal.hotrestart.impl.gc.record.Record;
import com.hazelcast.internal.hotrestart.impl.gc.record.RecordMap;

/**
 * Represents a chunk file.
 */
public abstract class Chunk implements Disposable {
    /** System property that specifies the limit on the value chunk file size */
    public static final String SYSPROP_VAL_CHUNK_SIZE_LIMIT = "hazelcast.hotrestart.val.chunk.size.limit";

    /** System property that specifies the limit on the tombstone chunk file size */
    public static final String SYSPROP_TOMB_CHUNK_SIZE_LIMIT = "hazelcast.hotrestart.tomb.chunk.size.limit";

    /** Default value chunk file size limit in bytes. */
    @SuppressWarnings("checkstyle:magicnumber")
    public static final int VAL_SIZE_LIMIT_DEFAULT = 8 << 20;

    /** Default tombstone chunk file size limit in bytes. */
    @SuppressWarnings("checkstyle:magicnumber")
    public static final int TOMB_SIZE_LIMIT_DEFAULT = 1 << 20;

    /** Chunk filename suffix. */
    public static final String FNAME_SUFFIX = ".chunk";

    /**
     * Suffix added to the chunk file while it is active. On restart this file
     * is the only one whose last entry may be incomplete. If the system failed while
     * it was being written out, the caller did not receive a successful response,
     * therefore that entry doesn't actually exist.
     */
    public static final String ACTIVE_FNAME_SUFFIX = ".active";

    /**
     * Suffix added to a chunk file while it is being written to during a GC cycle.
     * If system fails during GC, such file should not be considered during restart.
     */
    public static final String SURVIVOR_FNAME_SUFFIX = Chunk.FNAME_SUFFIX + ".survivor";

    /** Name of the base directory for value records. */
    public static final String VAL_BASEDIR = "value";

    /** Name of the base directory for tombstone records. */
    public static final String TOMB_BASEDIR = "tombstone";

    /** Unique sequence number of this chunk. */
    public final long seq;

    public final RecordMap records;
    /**
     * The amount of garbage in this chunk in bytes. It is incremented when the record is retired or in the
     * {@link com.hazelcast.internal.hotrestart.impl.gc.Rebuilder} if we encounter a stale record (encounter a record interred by
     * a prefix tombstone or a record which has a lower seq than the one we already encountered).
     * The value is used while calculating the benefit of GC-ing the chunk.
     */
    public long garbage;
    public int liveRecordCount;

    /** Will be true when a new prefix tombstone arrives and this chunk may contain records interred by it. */
    private boolean needsDismissing;

    public Chunk(long chunkSeq, RecordMap records) {
        this.seq = chunkSeq;
        this.records = records;
    }

    public Chunk(GrowingChunk from) {
        this.seq = from.seq;
        this.records = from.records;
        this.liveRecordCount = from.liveRecordCount;
        this.garbage = from.garbage;
        this.needsDismissing = from.needsDismissing();
    }

    public Chunk(long chunkSeq, RecordMap records, int liveRecordCount, long garbage) {
        this.seq = chunkSeq;
        this.records = records;
        this.liveRecordCount = liveRecordCount;
        this.garbage = garbage;
    }

    /**
     * @return whether the "dismiss prefix garbage" operation is pending for this chunk.
     */
    public final boolean needsDismissing() {
        return needsDismissing;
    }

    /**
     * Sets whether the "dismiss prefix garbage" operation is needed for this chunk.
     */
    public void needsDismissing(boolean needsDismissing) {
        this.needsDismissing = needsDismissing;
    }

    /**
     * @return size in bytes of the chunk file
     */
    public abstract long size();

    /**
     * Retires a record in this chunk (makes it dead).
     * @param kh key handle of the record
     * @param r the record to retire
     * @param mayIncrementGarbageCount whether it is appropriate to increment the garbage count for this key handle
     */
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

    /**
     * @return the filename suffix for this chunk
     */
    public String fnameSuffix() {
        return FNAME_SUFFIX;
    }

    /**
     * @return name of the base directory of this chunk (within the owning Hot Restart store's home dir)
     */
    public String base() {
        return VAL_BASEDIR;
    }

    public void dispose() {
        records.dispose();
    }

    /**
     * Fetches the configured size limit in bytes for the value chunk from the system property
     * {@value #SYSPROP_VAL_CHUNK_SIZE_LIMIT}.
     */
    public static int valChunkSizeLimit() {
        return Integer.getInteger(SYSPROP_VAL_CHUNK_SIZE_LIMIT, VAL_SIZE_LIMIT_DEFAULT);
    }

    /**
     * Fetches the configured size limit in bytes for the tombstone chunk from the system property
     * {@value #SYSPROP_TOMB_CHUNK_SIZE_LIMIT}.
     */
    public static int tombChunkSizeLimit() {
        return Integer.getInteger(SYSPROP_TOMB_CHUNK_SIZE_LIMIT, TOMB_SIZE_LIMIT_DEFAULT);
    }

    @Override
    public String toString() {
        return String.format("%s(%03x,%,d,%,d)",
                getClass().getSimpleName(), seq, liveRecordCount, garbage);
    }
}
