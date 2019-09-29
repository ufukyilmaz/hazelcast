package com.hazelcast.internal.hotrestart.impl;

import com.hazelcast.internal.hotrestart.KeyHandle;
import com.hazelcast.internal.hotrestart.RamStore;
import com.hazelcast.internal.hotrestart.impl.io.ChunkFileRecord;

/**
 * Item in the concurret queues during restarting. Reused along the complete pipeline:
 * <ol>
 *     <li>HotRestarter thread creates it and submits to a partition thread;</li>
 *     <li>partition thread attaches a key handle to it and submits to the Rebuilder thread;</li>
 *     <li>Rebuilder uses it to update the GC metadata and submits back to the partition thread, unless
 *         it determines that it corresponds to a record that is known to be garbage;</li>
 *     <li>partition thread inserts the value blob into the appropriate {@link RamStore}.</li>
 * </ol>
 */
public class RestartItem {
    public static final RestartItem END = new RestartItem.WithSetOfKeyHandle(0, null);
    private static final long RECORD_SEQ_CLEARED_ITEM = -1;

    public final long chunkSeq;
    public final long recordSeq;
    public final long prefix;
    public final int filePos;
    public final int size;
    public final byte[] key;
    public final byte[] value;

    public RamStore ramStore;
    public KeyHandle keyHandle;

    RestartItem(ChunkFileRecord rec) {
        this.chunkSeq = rec.chunkSeq();
        this.recordSeq = rec.recordSeq();
        this.prefix = rec.prefix();
        this.filePos = rec.filePos();
        this.size = rec.size();
        this.key = rec.key();
        this.value = rec.value();
    }

    private RestartItem(long chunkSeq, long recordSeq, long prefix, int filePos, int size) {
        this.chunkSeq = chunkSeq;
        this.recordSeq = recordSeq;
        this.prefix = prefix;
        this.filePos = filePos;
        this.size = size;
        this.key = null;
        this.value = null;
    }

    /** Says whether this is a special item, either:
     *  <ul>
     *      <li>describing a cleared record (interred by a prefix tombstone),</li>
     *      <li>requesting to remove null-keys from the {@code RamStore},</li>
     *      <li>or the "submitter gone" item, which the submitter thread sends as the very last item.</li>
     *  </ul>
     */
    public final boolean isSpecialItem() {
        return key == null;
    }

    /** @return whether this item corresponds to a cleared record (interred by a prefix tombstone) */
    public final boolean isClearedItem() {
        return recordSeq == RECORD_SEQ_CLEARED_ITEM;
    }

    /**
     * Constructs an item that corresponds to a cleared record (interred by a prefix tombstone).
     * @param rec the object representing the cleared record in a chunk file
     */
    public static RestartItem clearedItem(ChunkFileRecord rec) {
        return new RestartItem(rec.chunkSeq(), RECORD_SEQ_CLEARED_ITEM, rec.prefix(), rec.filePos(), rec.size());
    }

    /** Used both for the "remove null keys" item and for the "submitter gone" item. */
    public static class WithSetOfKeyHandle extends RestartItem {
        public final SetOfKeyHandle sokh;

        public WithSetOfKeyHandle(long prefix, SetOfKeyHandle sokh) {
            super(0, 0, prefix, 0, 0);
            this.sokh = sokh;
        }
    }
}
