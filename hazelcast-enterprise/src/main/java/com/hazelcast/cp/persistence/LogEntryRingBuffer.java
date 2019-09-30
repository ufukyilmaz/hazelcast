package com.hazelcast.cp.persistence;

import com.hazelcast.cp.internal.raft.impl.persistence.LogFileStructure;

import javax.annotation.Nonnull;

import static java.lang.Math.max;
import static java.lang.Math.min;

class LogEntryRingBuffer {
    private final long[] offsets;
    // A fixed formula determines for a given entry index its slot in offsets[].
    // bottomEntryIndex together with bufLen tells the range of entry indices
    // that are currently stored in offsets[]: [bottomEntryIndex .. bottomEntryIndex + bufLen - 1]
    private long bottomEntryIndex;
    private int bufLen;

    LogEntryRingBuffer(int capacity) {
        this.offsets = new long[capacity];
        this.bottomEntryIndex = 1;
    }

    LogEntryRingBuffer(int capacity, long firstEntryIndex) {
        this.offsets = new long[capacity];
        this.bottomEntryIndex = firstEntryIndex;
    }

    LogEntryRingBuffer(int size, @Nonnull LogFileStructure logFileStructure) {
        this.offsets = new long[size];
        importEntryOffsets(logFileStructure);
    }

    long topIndex() {
        return bottomEntryIndex + bufLen - 1;
    }

    void addEntryOffset(long offset) {
        int insertionPoint = (int) ((bottomEntryIndex + bufLen) % offsets.length);
        offsets[insertionPoint] = offset;
        if (bufLen == offsets.length) {
            bottomEntryIndex++;
        } else {
            bufLen++;
        }
    }

    long getEntryOffset(long entryIndex) {
        if (entryIndex < bottomEntryIndex || entryIndex > topIndex()) {
            throw new IndexOutOfBoundsException(
                    String.format("Asked for entry index %,d, available range is [%,d..%,d]",
                            entryIndex, bottomEntryIndex, topIndex()));
        }
        int idx = (int) (entryIndex % offsets.length);
        return offsets[idx];
    }

    long deleteEntriesFrom(long deletionStartIndex) {
        long offsetAtDeletionStart = getEntryOffset(deletionStartIndex);
        long newBufLen = deletionStartIndex - bottomEntryIndex;
        bufLen = (int) (max(0, min(bufLen, newBufLen)));
        return offsetAtDeletionStart;
    }

    /**
     * Called when receiving a snapshot and starting a new file with it. The
     * store puts the snapshot at the start of the new file and then copies all
     * the entries beyond the snapshot from the previous to the new file. This
     * method adjusts the file offsets of the copied entries and discards the
     * entries up to the snapshot.
     *
     * @param newStartOffset the start offset of the first entry to keep (snapshotIndex + 1)
     * @param snapshotIndex  the snapshot's entry index
     */
    void adjustToNewFile(long newStartOffset, long snapshotIndex) {
        if (bufLen > 0 && snapshotIndex < bottomEntryIndex) {
            throw new IndexOutOfBoundsException(
                    String.format("Snapshot index %,d is less than the lowest entry index in the buffer %,d",
                            snapshotIndex, bottomEntryIndex));
        }
        long newBottomIndex = snapshotIndex + 1;
        int indexDelta = (int) (newBottomIndex - bottomEntryIndex);
        bottomEntryIndex = newBottomIndex;
        bufLen -= indexDelta;
        if (bufLen <= 0) {
            bufLen = 0;
            return;
        }
        adjustOffsets(newStartOffset);
    }

    @Nonnull
    long[] exportEntryOffsets() {
        long[] result = new long[bufLen];
        int[] startAndEndIndex = startAndEndIndex();
        int startIndex = startAndEndIndex[0];
        int endIndex = startAndEndIndex[1];
        if (endIndex >= startIndex) {
            System.arraycopy(offsets, startIndex, result, 0, bufLen);
        } else {
            int firstPartLen = offsets.length - startIndex;
            System.arraycopy(offsets, startIndex, result, 0, firstPartLen);
            System.arraycopy(offsets, 0, result, firstPartLen, bufLen - firstPartLen);
        }
        return result;
    }

    private void importEntryOffsets(LogFileStructure logFileStructure) {
        this.bottomEntryIndex = logFileStructure.indexOfFirstTailEntry();
        long[] offsetsToImport = logFileStructure.tailEntryOffsets();
        this.bufLen = offsetsToImport.length;
        if (bufLen > offsets.length) {
            throw new IllegalArgumentException(String.format(
                    "Specified ring buffer capacity %,d is less than the number of offsets to import %,d.",
                    offsets.length, bufLen));
        }
        if (bufLen == 0) {
            return;
        }
        int[] startAndEndIndex = startAndEndIndex();
        int startIndex = startAndEndIndex[0];
        int endIndex = startAndEndIndex[1];
        if (endIndex >= startIndex) {
            System.arraycopy(offsetsToImport, 0, offsets, startIndex, offsetsToImport.length);
        } else {
            int firstPartLen = offsets.length - startIndex;
            System.arraycopy(offsetsToImport, 0, offsets, startIndex, firstPartLen);
            System.arraycopy(offsetsToImport, firstPartLen, offsets, 0, offsetsToImport.length - firstPartLen);
        }
    }

    private void adjustOffsets(long newStartOffset) {
        long startIndexLong = bottomEntryIndex % offsets.length;
        int startIndex = (int) startIndexLong;
        int limitIndex = (int) (startIndexLong + bufLen);
        long offsetDelta = newStartOffset - offsets[startIndex];
        for (int i = startIndex; i != limitIndex; i++) {
            offsets[i % offsets.length] += offsetDelta;
        }
    }

    private int[] startAndEndIndex() {
        long startIndexLong = bottomEntryIndex % offsets.length;
        long endIndexLong = (startIndexLong + bufLen - 1) % offsets.length;
        return new int[]{ (int) startIndexLong, (int) endIndexLong };
    }
}
