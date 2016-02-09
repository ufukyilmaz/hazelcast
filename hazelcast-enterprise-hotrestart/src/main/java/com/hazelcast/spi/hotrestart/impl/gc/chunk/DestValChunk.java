package com.hazelcast.spi.hotrestart.impl.gc.chunk;

import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.impl.gc.GcExecutor.MutatorCatchup;
import com.hazelcast.spi.hotrestart.impl.gc.GcHelper;
import com.hazelcast.spi.hotrestart.impl.gc.GcLogger;
import com.hazelcast.spi.hotrestart.impl.gc.record.GcRecord;
import com.hazelcast.spi.hotrestart.impl.gc.record.RecordDataHolder;
import com.hazelcast.spi.hotrestart.impl.gc.record.RecordMap;
import com.hazelcast.spi.hotrestart.impl.gc.record.RecordMapOnHeap;

import java.io.DataOutputStream;
import java.io.FileOutputStream;

import static com.hazelcast.spi.hotrestart.impl.gc.GcHelper.bufferedOutputStream;

/**
 * Destination chunk used by the {@link com.hazelcast.spi.hotrestart.impl.gc.Evacuator}.
 */
public final class DestValChunk extends WriteThroughChunk {

    public DestValChunk(long seq, RecordMap records, FileOutputStream out, GcHelper gcHelper) {
        super(seq, DEST_FNAME_SUFFIX, records, out, gcHelper);
    }

    @Override public void insertOrUpdate(long prefix, KeyHandle kh, long seq, int size, int ignored) {
        insertOrUpdateValue(prefix, kh, seq, size);
    }

    public final void add(GcRecord gcr, KeyHandle kh, RecordDataHolder holder, MutatorCatchup mc) {
        ((RecordMapOnHeap) records).put(kh, gcr);
        liveRecordCount++;
        final long filePosition = size;
        size += gcr.size();
        gcr.intoOut(dataOut, filePosition, kh, holder, mc);
    }

    @Override DataOutputStream dataOutputStream(FileOutputStream fileOut, GcHelper gch) {
        return new DataOutputStream(gch.compressionEnabled()
                ? gch.compressor.compressedOutputStream(fileOut)
                : bufferedOutputStream(fileOut));
    }

    @Override public StableValChunk toStableChunk() {
        return new StableValChunk(seq, gcHelper.toPlainRecordMap(records), liveRecordCount, size(),
                garbage, needsDismissing(), gcHelper.compressionEnabled());
    }
}
