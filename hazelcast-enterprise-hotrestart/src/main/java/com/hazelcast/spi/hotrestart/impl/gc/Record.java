package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.impl.gc.GcExecutor.MutatorCatchup;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.spi.hotrestart.impl.BufferingInputStream.BUFFER_SIZE;
import static com.hazelcast.spi.hotrestart.impl.BufferingInputStream.LOG_OF_BUFFER_SIZE;

/**
 * A record in the chunk file. Represents a single insert/update/delete event.
 */
public abstract class Record {
    /** Size of the value record header in a chunk file */
    public static final int VAL_HEADER_SIZE =
            LONG_SIZE_IN_BYTES + LONG_SIZE_IN_BYTES + INT_SIZE_IN_BYTES + INT_SIZE_IN_BYTES;
    /** Size of the tombstone record header in a chunk file */
    public static final int TOMB_HEADER_SIZE =
            LONG_SIZE_IN_BYTES + LONG_SIZE_IN_BYTES + INT_SIZE_IN_BYTES;

    final long deadOrAliveSeq() {
        final long seq = rawSeqValue();
        assert seq != 0 : "Record seq is zero";
        return Math.abs(seq);
    }

    final long liveSeq() {
        final long seq = rawSeqValue();
        assert seq > 0 : "liveSeq() called on a dead record. Raw seq is " + seq;
        return seq;
    }

    final long deadSeq() {
        final long seq = rawSeqValue();
        assert seq < 0 : "deadSeq() called on a live record. Raw seq is " + seq;
        return -seq;
    }

    final int size() {
        final int size = rawSizeValue();
        return size >= 0 ? size : -size;
    }

    final int payloadSize() {
        return size() - VAL_HEADER_SIZE;
    }

    final boolean isTombstone() {
        return rawSizeValue() < 0;
    }

    final boolean isAlive() {
        return rawSeqValue() > 0;
    }

    final void update(long seq, int size, boolean isTombstone) {
        setRawSeqSize(seq, toRawSizeValue(size, isTombstone));
    }

    final void retire(boolean mayIncrementGarbageCount) {
        assert isAlive() : "Attempt to retire a dead record";
        negateSeq();
        if (mayIncrementGarbageCount && !isTombstone()) {
            incrementGarbageCount();
        }
    }

    final long intoOut(DataOutputStream out, FileOutputStream fileOut, long filePosition, long prefix,
                       RecordDataHolder bufs, MutatorCatchup mc) {
        if (out == null) {
            return filePosition;
        }
        try {
            final ByteBuffer keyBuf = bufs.keyBuffer;
            final ByteBuffer valBuf = bufs.valueBuffer;
            final int keySize = keyBuf.remaining();
            final int valSize = valBuf.remaining();
            final long startPos = positionInUnitsOfBufsize(filePosition);
            out.writeLong(liveSeq());
            out.writeLong(prefix);
            out.writeInt(keySize);
            out.writeInt(isTombstone() ? -1 : valBuf.remaining());
            out.write(keyBuf.array(), keyBuf.position(), keySize);
            filePosition += VAL_HEADER_SIZE + keySize;
            if (positionInUnitsOfBufsize(filePosition) > startPos) {
                mc.catchupNow();
            }
            if (isTombstone() || valSize <= 0) {
                return filePosition;
            }
            do {
                final int alignedCount = BUFFER_SIZE - (int) (filePosition & BUFFER_SIZE - 1);
                final int transferredCount = Math.min(valBuf.remaining(), alignedCount);
                final int pos = valBuf.position();
                out.write(valBuf.array(), pos, transferredCount);
                if (transferredCount == alignedCount) {
                    mc.catchupNow();
                }
                valBuf.position(pos + transferredCount);
                filePosition += transferredCount;
            } while (valBuf.hasRemaining());
            return filePosition;
        } catch (IOException e) {
            throw new HotRestartException(e);
        }
    }

    @Override public String toString() {
        return String.format("%s(%03x)", getClass().getSimpleName(), liveSeq());
    }

    abstract long keyPrefix(KeyHandle kh);

    // If seq is zero, this object does not relate to any live record, but garbageCount
    // keeps tracking the number of garbage records on the same key in the same chunk.
    abstract long rawSeqValue();

    // The size field is not cleared on retirement: this is essential to the proper
    // function of Chunk#dismissGarbageRecord() (only called during GC for a record
    // which became garbage during the same GC cycle).
    abstract int rawSizeValue();

    abstract int garbageCount();

    abstract void negateSeq();

    abstract int decrementGarbageCount();

    abstract void incrementGarbageCount();

    abstract void setGarbageCount(int newCount);

    abstract void setRawSeqSize(long rawSeqValue, int rawSizeValue);

    public static int size(byte[] key, byte[] value) {
        return value != null ? VAL_HEADER_SIZE + key.length + value.length : TOMB_HEADER_SIZE + key.length;
    }

    static int toRawSizeValue(int size, boolean isTombstone) {
        return isTombstone ? -size : size;
    }

    static long positionInUnitsOfBufsize(long filePosition) {
        return filePosition >> LOG_OF_BUFFER_SIZE;
    }
}
