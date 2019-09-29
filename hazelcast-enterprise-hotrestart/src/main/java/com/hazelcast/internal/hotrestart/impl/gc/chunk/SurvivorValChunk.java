package com.hazelcast.internal.hotrestart.impl.gc.chunk;

import com.hazelcast.internal.hotrestart.KeyHandle;
import com.hazelcast.internal.hotrestart.impl.gc.GcHelper;
import com.hazelcast.internal.hotrestart.impl.gc.record.Record;
import com.hazelcast.internal.hotrestart.impl.gc.record.RecordDataHolder;
import com.hazelcast.internal.hotrestart.impl.gc.record.RecordMap;
import com.hazelcast.internal.hotrestart.impl.io.ChunkFileOut;

/** Survivor chunk used by the {@link com.hazelcast.internal.hotrestart.impl.gc.ValEvacuator}. */
public final class SurvivorValChunk extends WriteThroughChunk {

    public SurvivorValChunk(long seq, RecordMap records, ChunkFileOut out, GcHelper gcHelper) {
        super(seq, SURVIVOR_FNAME_SUFFIX, records, out, gcHelper);
    }

    /**
     * Adds a record both to the survivor chunk file and to the chunk's record map.
     * @param r the record
     * @param kh record's key handle
     * @param holder record's key and value blobs
     */
    public void add(Record r, KeyHandle kh, RecordDataHolder holder) {
        final long prefix = r.keyPrefix(kh);
        records.putIfAbsent(prefix, kh, r.liveSeq(), r.size(), false, 0);
        liveRecordCount++;
        grow(r.size());
        out.writeValueRecord(r, prefix, holder.keyBuffer, holder.valueBuffer);
    }

    @Override
    public void insertOrUpdate(long recordSeq, long keyPrefix, KeyHandle kh, int ignored, int size) {
        throw new UnsupportedOperationException("SurvivorValChunk#insertOrUpdate");
    }

    @Override
    protected int determineSizeLimit() {
        return valChunkSizeLimit();
    }

    @Override
    public StableValChunk toStableChunk() {
        return new StableValChunk(seq, records.toStable(), liveRecordCount, size(), garbage, needsDismissing());
    }
}
