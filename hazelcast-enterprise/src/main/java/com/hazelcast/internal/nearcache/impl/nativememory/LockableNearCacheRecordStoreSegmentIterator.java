package com.hazelcast.internal.nearcache.impl.nativememory;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.serialization.Data;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Iterates over the keys of an array of {@link HDNearCacheRecordStoreImpl} instances.
 *
 * The iterator dynamically locks and unlocks the provided {@link LockableNearCacheRecordStoreSegment}.
 *
 * This {@link Iterator} is <b>not</b> thread-safe! It also doesn't support the {@link Iterator#remove()} method.
 */
class LockableNearCacheRecordStoreSegmentIterator implements Iterator<Data>, Closeable {

    private final Thread ownerThread = Thread.currentThread();

    private final HDNearCacheRecordStoreImpl[] segments;
    private final boolean[] obtainedLocks;

    private int segmentIndex = -1;

    private LockableNearCacheRecordStoreSegment segment;
    private Iterator<Data> segmentKeySetIterator;

    LockableNearCacheRecordStoreSegmentIterator(HDNearCacheRecordStoreImpl[] segments) {
        this.segments = segments;
        this.obtainedLocks = new boolean[segments.length];
    }

    @Override
    public boolean hasNext() {
        if (segmentKeySetIterator == null) {
            // init lazy to keep the lock window as small as possible
            lockNextSegment();
        }
        if (segmentKeySetIterator.hasNext()) {
            return true;
        }
        // the actual segment has been iterated, let's check if there is another one
        unlockCurrentSegment();
        if (!lockNextSegment()) {
            return false;
        }
        // call this method recursively to handle the unlock if the new segment is empty
        return hasNext();
    }

    @Override
    public Data next() {
        if (segmentKeySetIterator == null) {
            // init lazy to keep the lock window as small as possible
            lockNextSegment();
        }
        while (!segmentKeySetIterator.hasNext()) {
            // the actual segment has been iterated, let's check if there is another one
            unlockCurrentSegment();
            if (!lockNextSegment()) {
                throw new NoSuchElementException();
            }
        }
        return segmentKeySetIterator.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        int foundLocks = 0;
        for (int i = 0; i < obtainedLocks.length; i++) {
            if (obtainedLocks[i]) {
                ((LockableNearCacheRecordStoreSegment) segments[i]).unlock();
                foundLocks++;
            }
        }
        if (foundLocks > 0) {
            throw new IOException("Found " + foundLocks + " locked segments in LockableNearCacheRecordStoreSegmentIterator!");
        }
    }

    @SuppressWarnings("unchecked")
    private boolean lockNextSegment() {
        if (Thread.currentThread() != ownerThread) {
            throw new HazelcastException("LockableNearCacheRecordStoreSegmentIterator is not thread-safe!"
                    + " Don't hand it over to another thread during usage.");
        }
        if (++segmentIndex >= segments.length) {
            return false;
        }

        segment = (LockableNearCacheRecordStoreSegment) segments[segmentIndex];

        obtainedLocks[segmentIndex] = true;
        segment.lock();

        segmentKeySetIterator = segment.getKeySetIterator();

        return true;
    }

    private void unlockCurrentSegment() {
        if (Thread.currentThread() != ownerThread) {
            throw new HazelcastException("LockableNearCacheRecordStoreSegmentIterator is not thread-safe!"
                    + " Don't hand it over to another thread during usage.");
        }
        if (segmentIndex >= segments.length) {
            return;
        }

        segment.unlock();
        obtainedLocks[segmentIndex] = false;
    }
}
