package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.hotrestart.impl.ChunkFileCursor;
import com.hazelcast.spi.hotrestart.impl.gc.RecordMap.Cursor;
import com.hazelcast.util.collection.LongHashSet;
import com.hazelcast.util.counters.Counter;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import static com.hazelcast.spi.hotrestart.impl.gc.GcHelper.bufferedOutputStream;
import static com.hazelcast.spi.hotrestart.impl.gc.GcHelper.closeIgnoringFailure;
import static com.hazelcast.spi.hotrestart.impl.gc.WriteThroughTombChunk.writeTombstone;

/**
 * Represents a tombstone chunk whose on-disk contents are stable (immutable).
 */
final class StableTombChunk extends StableChunk {

    StableTombChunk(WriteThroughTombChunk from, boolean compressed) {
        super(from, compressed);
    }

    StableTombChunk(long seq, RecordMap records, int liveRecordCount, long size, long garbage) {
        super(seq, records, liveRecordCount, size, garbage, false, false);
    }

    @Override String base() {
        return TOMB_BASEDIR;
    }

    public void compactFile(GcHelper gcHelper, Counter tombOccupancy, Counter tombGarbage) {
        final LongHashSet liveSeqs = new LongHashSet(liveRecordCount, 0L);
        for (Cursor cursor = records.cursor(); cursor.advance();) {
            final Record r = cursor.asRecord();
            if (!r.isAlive()) {
                continue;
            }
            liveSeqs.add(r.liveSeq());
        }
        final File currFile = gcHelper.chunkFile(this, false);
        final FileOutputStream fileOut = gcHelper.createFileOutputStream(base(), seq, DEST_FNAME_SUFFIX);
        final DataOutputStream dataOut = new DataOutputStream(bufferedOutputStream(fileOut));
        try {
            for (ChunkFileCursor.Tomb fc = new ChunkFileCursor.Tomb(currFile, gcHelper); fc.advance();) {
                if (liveSeqs.contains(fc.recordSeq())) {
                    writeTombstone(dataOut, fc.recordSeq(), fc.prefix(), fc.key());
                }
            }
            dataOut.flush();
            fileOut.getFD().sync();
            dataOut.close();
            gcHelper.changeSuffix(base(), seq, DEST_FNAME_SUFFIX, FNAME_SUFFIX);
            tombOccupancy.inc(-garbage);
            tombGarbage.inc(-garbage);
            size -= garbage;
            garbage = 0;
        } catch (IOException e) {
            throw new HotRestartException(String.format("Failed to compact tombstone chunk #%x", seq), e);
        } finally {
            closeIgnoringFailure(dataOut);
        }
    }
}
