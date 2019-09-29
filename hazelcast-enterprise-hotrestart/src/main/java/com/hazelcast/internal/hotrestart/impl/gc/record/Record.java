package com.hazelcast.internal.hotrestart.impl.gc.record;

import com.hazelcast.internal.hotrestart.KeyHandle;

import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.internal.hotrestart.impl.HotRestarter.LOG_OF_BUFFER_SIZE;

/**
 * Holds the metadata on the newest record in a chunk associated with a given key and also
 * some metadata on any dead records within the same chunk associated with the same key.
 */
public abstract class Record {
    /**
     * Size of the value record header in a chunk file: seq, prefix, key size, value size
     */
    public static final int VAL_HEADER_SIZE =
            LONG_SIZE_IN_BYTES + LONG_SIZE_IN_BYTES + INT_SIZE_IN_BYTES + INT_SIZE_IN_BYTES;
    /**
     * Size of the tombstone record header in a chunk file: seq, prefix, key size
     */
    public static final int TOMB_HEADER_SIZE =
            LONG_SIZE_IN_BYTES + LONG_SIZE_IN_BYTES + INT_SIZE_IN_BYTES;

    /**
     * @return sequence number of this record, whether the record is alive or dead
     */
    public final long deadOrAliveSeq() {
        final long seq = rawSeqValue();
        assert seq != 0 : "Record seq is zero";
        return Math.abs(seq);
    }

    /**
     * Asserts that the record is alive and returns its sequence number.
     */
    public final long liveSeq() {
        final long seq = rawSeqValue();
        assert seq > 0 : "liveSeq() called on a dead record. Raw seq is " + seq;
        return seq;
    }

    /**
     * Asserts that the record is dead and returns its sequence number.
     */
    public final long deadSeq() {
        final long seq = rawSeqValue();
        assert seq < 0 : "deadSeq() called on a live record. Raw seq is " + seq;
        return -seq;
    }

    /**
     * @return size in bytes of this record
     */
    public final int size() {
        return Math.abs(rawSizeValue());
    }

    /**
     * @return size in bytes of this record's payload (exluding the header)
     */
    public final int payloadSize() {
        return size() - VAL_HEADER_SIZE;
    }

    public final boolean isTombstone() {
        return rawSizeValue() < 0;
    }

    public boolean isAlive() {
        return rawSeqValue() > 0;
    }

    /**
     * @return the number of dead records with this record's key in this record's chunk.
     */
    public final int garbageCount() {
        return isTombstone() ? 0 : additionalInt();
    }

    /**
     * Asserts that this is a tombstone record and returns its offset in the underlying chunk file.
     */
    public final int filePosition() {
        assert isTombstone() : "Attempt to retrieve file position of a value record";
        return additionalInt();
    }

    /**
     * Asserts that this is a tombstone record and sets its offset in the underlying chunk file
     * to the supplied value.
     */
    public final void setFilePosition(int filePosition) {
        assert isTombstone() : "Attempt to set file position on a value record";
        setAdditionalInt(filePosition);
    }

    /**
     * Updates the sequence number and size properties of this record to the supplied values.
     */
    public final void update(long seq, int size) {
        setRawSeqSize(seq, toRawSizeValue(size, isTombstone()));
    }

    /**
     * Retires this record (makes it dead).
     *
     * @param mayIncrementGarbageCount whether it is appropriate to increment the record's garbage count property
     *                                 (it is not appropriate to increment if the record is being retired due to
     *                                 a prefix tombstone)
     */
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

    /**
     * @param kh the key handle associated with this record
     * @return the key prefix associated with this record
     */
    public abstract long keyPrefix(KeyHandle kh);

    /**
     * Sets the raw "seq" and "size" values on this record.
     *
     * @see #rawSeqValue()
     * @see #rawSizeValue()
     */
    public abstract void setRawSeqSize(long rawSeqValue, int rawSizeValue);

    /**
     * @return the value of the raw, low-level "seq" property which encodes both the
     * record's sequence number and whether it's alive.
     * If zero, then this object does not relate to any live record, but {@code garbageCount}
     * keeps tracking the number of garbage records on the same key in the same chunk.
     */
    public abstract long rawSeqValue();

    /**
     * @return the raw, low-level "size" property which encodes both the size of the record
     * and whether it's a tombstone.
     */
    public abstract int rawSizeValue();

    /**
     * Sets the raw "additional int" property on this record.
     *
     * @see #additionalInt()
     */
    public abstract void setAdditionalInt(int value);

    /**
     * @return the "additional int" property which, for a value record, denotes the garbage count
     * and for a tombstone denotes file position.
     */
    public abstract int additionalInt();

    /**
     * Negates the value of the {@code rawSeq} property.
     */
    public abstract void negateSeq();

    /**
     * Decrements this record's garbage count.
     */
    public abstract int decrementGarbageCount();

    /**
     * Increments this record's garbage count.
     */
    public abstract void incrementGarbageCount();

    /**
     * Sets this record's garbage count to the given value.
     */
    public final void setGarbageCount(int newCount) {
        assert newCount == 0 || !isTombstone() : "Attempt to set non-zero garbage count on a tombstone";
        setAdditionalInt(newCount);
    }

    /**
     * Calculates the size in bytes a record would have in a chunk file if it contained
     * the provided key and value blobs.
     *
     * @param key   the key blob
     * @param value the value blob (a {@code null} value signals a tombstone record)
     */
    public static int size(byte[] key, byte[] value) {
        return value != null ? VAL_HEADER_SIZE + key.length + value.length : TOMB_HEADER_SIZE + key.length;
    }

    /**
     * Encodes the given {@code size} and {@code isTombstone} parameters into a single {@code int} value
     * of the raw "size" property.
     */
    public static int toRawSizeValue(int size, boolean isTombstone) {
        return isTombstone ? -size : size;
    }

    /**
     * Expresses the given file position in the unit of
     * {@link com.hazelcast.internal.hotrestart.impl.HotRestarter#BUFFER_SIZE}. Used during writing to a chunk
     * file to determine when another buffer full of data was written out.
     */
    public static long positionInUnitsOfBufsize(long filePosition) {
        return filePosition >> LOG_OF_BUFFER_SIZE;
    }
}
