package com.hazelcast.spi.hotrestart.impl;

import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.RamStore;
import com.hazelcast.spi.hotrestart.impl.io.ChunkFileRecord;

/**
 * Acts as an item in the concurret queues during restarting.
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

    public final boolean isSpecialItem() {
        return key == null;
    }

    public final boolean isClearedItem() {
        return recordSeq == RECORD_SEQ_CLEARED_ITEM;
    }

    public static RestartItem clearedItem(ChunkFileRecord rec) {
        return new RestartItem(rec.chunkSeq(), RECORD_SEQ_CLEARED_ITEM, rec.prefix(), rec.filePos(), rec.size());
    }

    public static class WithSetOfKeyHandle extends RestartItem {
        public final SetOfKeyHandle sokh;

        public WithSetOfKeyHandle(long prefix, SetOfKeyHandle sokh) {
            super(0, 0, prefix, 0, 0);
            this.sokh = sokh;
        }
    }
}
