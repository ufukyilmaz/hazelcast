package com.hazelcast.spi.hotrestart.impl.gc.record;

import com.hazelcast.spi.hotrestart.KeyHandle;

import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.spi.hotrestart.impl.io.BufferingInputStream.LOG_OF_BUFFER_SIZE;

/**
 * A record in the chunk file. Represents a single insert/update/delete event.
 */
public abstract class Record {
    /** Size of the value record header in a chunk file: seq, prefix, key size, value size */
    public static final int VAL_HEADER_SIZE =
            LONG_SIZE_IN_BYTES + LONG_SIZE_IN_BYTES + INT_SIZE_IN_BYTES + INT_SIZE_IN_BYTES;
    /** Size of the tombstone record header in a chunk file: seq, prefix, key size */
    public static final int TOMB_HEADER_SIZE =
            LONG_SIZE_IN_BYTES + LONG_SIZE_IN_BYTES + INT_SIZE_IN_BYTES;

    public final long deadOrAliveSeq() {
        final long seq = rawSeqValue();
        assert seq != 0 : "Record seq is zero";
        return Math.abs(seq);
    }

    public final long liveSeq() {
        final long seq = rawSeqValue();
        assert seq > 0 : "liveSeq() called on a dead record. Raw seq is " + seq;
        return seq;
    }

    public final long deadSeq() {
        final long seq = rawSeqValue();
        assert seq < 0 : "deadSeq() called on a live record. Raw seq is " + seq;
        return -seq;
    }

    public final int size() {
        return Math.abs(rawSizeValue());
    }

    public final int payloadSize() {
        return size() - VAL_HEADER_SIZE;
    }

    public final boolean isTombstone() {
        return rawSizeValue() < 0;
    }

    public boolean isAlive() {
        return rawSeqValue() > 0;
    }

    public final int garbageCount() {
        return isTombstone() ? 0 : additionalInt();
    }

    public final int filePosition() {
        assert isTombstone() : "Attempt to retrieve file position of a value record";
        return additionalInt();
    }

    public final void setFilePosition(int filePosition) {
        assert isTombstone() : "Attempt to set file position on a value record";
        setAdditionalInt(filePosition);
    }

    public final void update(long seq, int size) {
        setRawSeqSize(seq, toRawSizeValue(size, isTombstone()));
    }

    public final void retire(boolean mayIncrementGarbageCount) {
        assert isAlive() : "Attempt to retire a dead record";
        negateSeq();
        if (mayIncrementGarbageCount && !isTombstone()) {
            incrementGarbageCount();
        }
    }


    @Override
    public String toString() {
        return String.format("%s(%03x)", getClass().getSimpleName(), liveSeq());
    }

    public abstract long keyPrefix(KeyHandle kh);

    // If seq is zero, this object does not relate to any live record, but garbageCount
    // keeps tracking the number of garbage records on the same key in the same chunk.
    public abstract long rawSeqValue();

    // The size field is not cleared on retirement: this is essential to the proper
    // function of Chunk#dismissGarbageRecord() (only called during GC for a record
    // which became garbage during the same GC cycle).
    public abstract int rawSizeValue();

    public abstract int additionalInt();

    public abstract void negateSeq();

    public abstract int decrementGarbageCount();

    public abstract void incrementGarbageCount();

    public final void setGarbageCount(int newCount) {
        assert newCount == 0 || !isTombstone() : "Attempt to set non-zero garbage count on a tombstone";
        setAdditionalInt(newCount);
    }

    public abstract void setAdditionalInt(int value);

    public abstract void setRawSeqSize(long rawSeqValue, int rawSizeValue);

    public static int size(byte[] key, byte[] value) {
        return value != null ? VAL_HEADER_SIZE + key.length + value.length : TOMB_HEADER_SIZE + key.length;
    }

    public static int toRawSizeValue(int size, boolean isTombstone) {
        return isTombstone ? -size : size;
    }

    public static long positionInUnitsOfBufsize(long filePosition) {
        return filePosition >> LOG_OF_BUFFER_SIZE;
    }
}
