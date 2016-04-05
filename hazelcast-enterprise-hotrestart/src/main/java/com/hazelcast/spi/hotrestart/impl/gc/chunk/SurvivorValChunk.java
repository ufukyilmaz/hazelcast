package com.hazelcast.spi.hotrestart.impl.gc.chunk;

import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.impl.gc.GcHelper;
import com.hazelcast.spi.hotrestart.impl.gc.record.Record;
import com.hazelcast.spi.hotrestart.impl.gc.record.RecordDataHolder;
import com.hazelcast.spi.hotrestart.impl.gc.record.RecordMap;
import com.hazelcast.spi.hotrestart.impl.io.ChunkFileOut;

/**
 * Destination chunk used by the {@link com.hazelcast.spi.hotrestart.impl.gc.ValEvacuator}.
 */
public final class SurvivorValChunk extends WriteThroughChunk {

    public SurvivorValChunk(long seq, RecordMap records, ChunkFileOut out, GcHelper gcHelper) {
        super(seq, DEST_FNAME_SUFFIX, records, out, gcHelper);
    }

    @Override public void insertOrUpdate(long prefix, KeyHandle kh, long seq, int size, int ignored) {
        insertOrUpdateValue(prefix, kh, seq, size);
    }

    @Override protected int determineSizeLimit() {
        return valChunkSizeLimit();
    }

    public void add(Record r, KeyHandle kh, RecordDataHolder holder) {
        final long prefix = r.keyPrefix(kh);
        records.putIfAbsent(prefix, kh, r.liveSeq(), r.size(), false, 0);
        liveRecordCount++;
        size += r.size();
        out.writeValueRecord(r, prefix, holder.keyBuffer, holder.valueBuffer);
    }

    @Override public StableValChunk toStableChunk() {
        return new StableValChunk(seq, records.toStable(), liveRecordCount, size(), garbage, needsDismissing());
    }
}
