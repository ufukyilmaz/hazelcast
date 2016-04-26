package com.hazelcast.spi.hotrestart.impl;

import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.RamStore;

/**
 * Acts as an item in the concurret queues during restarting.
 */
public class RestartItem {
    public static final RestartItem END = new RestartItem.WithSetOfKeyHandle(0, null);
    public static final int PREFIX_CLEARED_ITEM = -1;

    public final long chunkSeq;
    public final long prefix;
    public final long recordSeq;
    public final int size;
    public final byte[] key;
    public final byte[] value;

    public RamStore ramStore;
    public KeyHandle keyHandle;

    RestartItem(long chunkSeq, long prefix, long recordSeq, int size, byte[] key, byte[] value) {
        this.chunkSeq = chunkSeq;
        this.prefix = prefix;
        this.recordSeq = recordSeq;
        this.size = size;
        this.key = key;
        this.value = value;
    }

    public boolean isSpecialItem() {
        return key == null;
    }

    public boolean isClearedItem() {
        return prefix == PREFIX_CLEARED_ITEM;
    }

    public static RestartItem clearedItem(long chunkSeq, long recordSeq, int size) {
        return new RestartItem(chunkSeq, PREFIX_CLEARED_ITEM, recordSeq, size, null, null);
    }

    public static class WithSetOfKeyHandle extends RestartItem {
        public final SetOfKeyHandle sokh;

        public WithSetOfKeyHandle(long prefix, SetOfKeyHandle sokh) {
            super(0, prefix, 0, 0, null, null);
            this.sokh = sokh;
        }
    }
}
