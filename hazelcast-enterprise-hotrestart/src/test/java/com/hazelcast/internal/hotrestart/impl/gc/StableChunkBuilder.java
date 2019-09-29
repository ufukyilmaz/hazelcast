package com.hazelcast.internal.hotrestart.impl.gc;

import com.hazelcast.internal.hotrestart.impl.gc.chunk.StableValChunk;
import com.hazelcast.internal.hotrestart.impl.gc.record.RecordMap;
import com.hazelcast.internal.hotrestart.impl.gc.record.RecordMapOnHeap;

class StableChunkBuilder {

    private long seq;
    private RecordMap records = new RecordMapOnHeap();
    private int liveRecordCount;
    private long size;
    private long garbage;
    private boolean needsDismissing;
    private boolean compressed;

    private StableChunkBuilder() {
    }

    static StableChunkBuilder chunkBuilder() {
        return new StableChunkBuilder();
    }

    StableValChunk build() {
        return new StableValChunk(seq, records, liveRecordCount, size, garbage, needsDismissing);
    }

    StableChunkBuilder compressed(boolean compressed) {
        this.compressed = compressed;
        return this;
    }

    StableChunkBuilder garbage(long garbage) {
        this.garbage = garbage;
        return this;
    }

    StableChunkBuilder liveRecordCount(int liveRecordCount) {
        this.liveRecordCount = liveRecordCount;
        return this;
    }

    StableChunkBuilder needsDismissing(boolean needsDismissing) {
        this.needsDismissing = needsDismissing;
        return this;
    }

    StableChunkBuilder records(RecordMap records) {
        this.records = records;
        return this;
    }

    StableChunkBuilder seq(long seq) {
        this.seq = seq;
        return this;
    }

    StableChunkBuilder size(long size) {
        this.size = size;
        return this;
    }
}
