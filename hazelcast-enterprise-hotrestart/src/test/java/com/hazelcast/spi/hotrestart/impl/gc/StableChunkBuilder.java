package com.hazelcast.spi.hotrestart.impl.gc;

class StableChunkBuilder {
    private long seq;
    private RecordMap records = new RecordMapOnHeap();
    private int liveRecordCount;
    private long youngestRecordSeq;
    private long size;
    private long garbage;
    private boolean needsDismissing;
    private boolean compressed;

    private StableChunkBuilder() { }

    static StableChunkBuilder chunkBuilder() {
        return new StableChunkBuilder();
    }

    StableChunk build() {
        return new StableChunk(
                seq, records, liveRecordCount, youngestRecordSeq, size, garbage, needsDismissing, compressed);
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

    StableChunkBuilder youngestRecordSeq(long youngestRecordSeq) {
        this.youngestRecordSeq = youngestRecordSeq;
        return this;
    }
}
