package com.hazelcast.internal.nearcache.impl.nativememory;

import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.nearcache.impl.nativememory.SegmentedNativeMemoryNearCacheRecordStore.DEFAULT_EXPIRY_SAMPLE_COUNT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category(QuickTest.class)
public class LockableNearCacheRecordStoreSegmentIteratorTest {

    private static final Data FIRST_DATA = mock(Data.class);
    private static final Data SECOND_DATA = mock(Data.class);

    private LockableNearCacheRecordStoreSegmentIterator iterator;

    @Test
    public void testIterator_whenAllLocksAreReleased_thenCloseThrowsNoException() throws Exception {
        LockableTestSegment firstSegment = new LockableTestSegment(FIRST_DATA, false);
        LockableTestSegment secondSegment = new LockableTestSegment(SECOND_DATA, false);
        iterator = new LockableNearCacheRecordStoreSegmentIterator(new NativeMemoryNearCacheRecordStore[]{
                firstSegment, secondSegment,
        });

        // in the beginning no segment is locked
        assertFalse(firstSegment.isLocked);
        assertFalse(secondSegment.isLocked);

        // the first hasNext() locks the first segment
        assertTrue(iterator.hasNext());
        assertTrue(firstSegment.isLocked);
        assertFalse(secondSegment.isLocked);

        // the data from the first segment is returned
        assertEquals(FIRST_DATA, iterator.next());

        // the second hasNext() unlocks the first and locks the second segment
        assertTrue(iterator.hasNext());
        assertFalse(firstSegment.isLocked);
        assertTrue(secondSegment.isLocked);

        // the data from the second segment is returned
        assertEquals(SECOND_DATA, iterator.next());

        // the last hasNext() unlocks the second segment
        assertFalse(iterator.hasNext());
        assertFalse(firstSegment.isLocked);
        assertFalse(secondSegment.isLocked);

        // closing the iterator succeeds without exception
        iterator.close();
    }

    @Test(expected = IOException.class)
    public void testIterator_whenUnlockFails_thenCloseThrowsIOException() throws Exception {
        LockableTestSegment segment = new LockableTestSegment(FIRST_DATA, true);
        iterator = new LockableNearCacheRecordStoreSegmentIterator(new NativeMemoryNearCacheRecordStore[]{
                segment,
        });

        // in the beginning the segment is not locked
        assertFalse(segment.isLocked);

        // the first hasNext() locks the segment
        assertTrue(iterator.hasNext());
        assertTrue(segment.isLocked);

        // the data from the segment is returned
        assertEquals(FIRST_DATA, iterator.next());

        // the second hasNext() throws an exception, so the segment will not be unlocked properly
        try {
            iterator.hasNext();
            fail("expected HazelcastException");
        } catch (HazelcastException ignored) {
        }
        assertTrue(segment.isLocked);

        // closing the iterator finally unlock the segment, which throws IOException
        iterator.close();
    }

    @Test
    public void test_whenNextIsCalledWithoutHasNext_thenSegmentIsInitializedAndLocked() {
        LockableTestSegment segment = new LockableTestSegment(FIRST_DATA, false);
        iterator = new LockableNearCacheRecordStoreSegmentIterator(new NativeMemoryNearCacheRecordStore[]{
                segment,
        });

        assertEquals(FIRST_DATA, iterator.next());
        assertTrue(segment.isLocked);

        assertFalse(iterator.hasNext());
        assertFalse(segment.isLocked);
    }

    @Test
    public void test_whenNextIsCalledWithoutHasNext_thenNextSegmentIsInitializedAndLocked() {
        LockableTestSegment firstSegment = new LockableTestSegment(FIRST_DATA, false);
        LockableTestSegment secondSegment = new LockableTestSegment(SECOND_DATA, false);
        iterator = new LockableNearCacheRecordStoreSegmentIterator(new NativeMemoryNearCacheRecordStore[]{
                firstSegment, secondSegment,
        });

        assertTrue(iterator.hasNext());
        assertTrue(firstSegment.isLocked);
        assertFalse(secondSegment.isLocked);

        assertEquals(FIRST_DATA, iterator.next());
        assertEquals(SECOND_DATA, iterator.next());

        assertFalse(iterator.hasNext());
        assertFalse(firstSegment.isLocked);
        assertFalse(secondSegment.isLocked);
    }

    @Test
    public void test_whenNextIsCalledAfterLastElement_thenNoSuchElementExceptionIsThrown() {
        LockableTestSegment segment = new LockableTestSegment(FIRST_DATA, false);
        iterator = new LockableNearCacheRecordStoreSegmentIterator(new NativeMemoryNearCacheRecordStore[]{
                segment,
        });

        assertTrue(iterator.hasNext());
        assertTrue(segment.isLocked);
        assertEquals(FIRST_DATA, iterator.next());

        assertFalse(iterator.hasNext());
        assertFalse(segment.isLocked);

        try {
            iterator.next();
            fail("expected NoSuchElementException");
        } catch (NoSuchElementException ignore) {
        }

        // check that the segment has not been locked again
        assertFalse(segment.isLocked);
    }

    @Test
    public void test_whenNextIsCalledAfterLastElement_thenEmptySegmentsAreSkipped() {
        LockableTestSegment firstEmptySegment = new LockableTestSegment();
        LockableTestSegment secondEmptySegment = new LockableTestSegment();
        LockableTestSegment segment = new LockableTestSegment(FIRST_DATA, false);
        iterator = new LockableNearCacheRecordStoreSegmentIterator(new NativeMemoryNearCacheRecordStore[]{
                firstEmptySegment,
                secondEmptySegment,
                segment,
        });

        // check that the empty segments are skipped correctly
        assertEquals(FIRST_DATA, iterator.next());

        assertFalse(iterator.hasNext());
        assertFalse(firstEmptySegment.isLocked);
        assertFalse(secondEmptySegment.isLocked);
        assertFalse(segment.isLocked);

        try {
            iterator.next();
            fail("expected NoSuchElementException");
        } catch (NoSuchElementException ignore) {
        }

        // check that the segments have not been locked again
        assertFalse(firstEmptySegment.isLocked);
        assertFalse(secondEmptySegment.isLocked);
        assertFalse(segment.isLocked);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRemove() {
        iterator = new LockableNearCacheRecordStoreSegmentIterator(new NativeMemoryNearCacheRecordStore[0]);

        iterator.remove();
    }

    @Test
    public void testThreadSafety_whenLockingIsCalledFromOtherThread_thenThrowException() throws Exception {
        LockableTestSegment segment = new LockableTestSegment(FIRST_DATA, false);
        iterator = new LockableNearCacheRecordStoreSegmentIterator(new NativeMemoryNearCacheRecordStore[]{
                segment,
        });

        // try to lock the segment from another thread
        final AtomicBoolean exceptionThrown = new AtomicBoolean();
        final CountDownLatch latch = new CountDownLatch(1);
        Thread thread = new Thread() {
            @Override
            public void run() {
                try {
                    iterator.hasNext();
                } catch (HazelcastException e) {
                    exceptionThrown.set(true);
                }
                latch.countDown();
            }
        };
        thread.start();
        thread.join();
        latch.await();

        // the other thread should not be able to lock the segment
        assertTrue(exceptionThrown.get());
        assertFalse(segment.isLocked);

        // the actual thread should be able to do so
        assertTrue(iterator.hasNext());
        assertTrue(segment.isLocked);
    }

    @Test
    public void testThreadSafety_whenUnlockingIsCalledFromOtherThread_thenThrowException() throws Exception {
        LockableTestSegment firstSegment = new LockableTestSegment(FIRST_DATA, false);
        LockableTestSegment secondSegment = new LockableTestSegment();
        iterator = new LockableNearCacheRecordStoreSegmentIterator(new NativeMemoryNearCacheRecordStore[]{
                firstSegment, secondSegment,
        });

        // the actual thread locks the first segment
        assertTrue(iterator.hasNext());
        assertEquals(FIRST_DATA, iterator.next());
        assertTrue(firstSegment.isLocked);
        assertFalse(secondSegment.isLocked);

        // try to unlock the first segment from another thread
        final AtomicBoolean exceptionThrown = new AtomicBoolean();
        final CountDownLatch latch = new CountDownLatch(1);
        Thread thread = new Thread() {
            @Override
            public void run() {
                try {
                    iterator.hasNext();
                } catch (HazelcastException e) {
                    exceptionThrown.set(true);
                }
                latch.countDown();
            }
        };
        thread.start();
        thread.join();
        latch.await();

        // the other thread should not be able to unlock the first segment
        assertTrue(exceptionThrown.get());
        assertTrue(firstSegment.isLocked);
        assertFalse(secondSegment.isLocked);

        // the actual thread should be able to do so
        assertFalse(iterator.hasNext());
        assertFalse(firstSegment.isLocked);
        assertFalse(secondSegment.isLocked);
    }

    private static class LockableTestSegment
            extends NativeMemoryNearCacheRecordStore
            implements LockableNearCacheRecordStoreSegment {

        private final Iterator<Data> iterator;

        private boolean throwExceptionOnUnlock;
        private boolean isLocked;

        LockableTestSegment() {
            this(new EmptyIterator(), false);
        }

        LockableTestSegment(Data data, boolean throwExceptionOnUnlock) {
            this(new SingleElementIterator(data), throwExceptionOnUnlock);
        }

        private LockableTestSegment(Iterator<Data> iterator, boolean throwExceptionOnUnlock) {
            super(new NearCacheConfig(), null, null, DEFAULT_EXPIRY_SAMPLE_COUNT);

            this.iterator = iterator;
            this.throwExceptionOnUnlock = throwExceptionOnUnlock;
        }

        @Override
        public Iterator<Data> getKeySetIterator() {
            return iterator;
        }

        @Override
        public void lock() {
            isLocked = true;
        }

        @Override
        public void unlock() {
            if (throwExceptionOnUnlock) {
                throwExceptionOnUnlock = false;
                throw new HazelcastException("expected");
            }
            isLocked = false;
        }
    }

    private static class SingleElementIterator implements Iterator<Data> {

        private final Data data;

        private boolean hasNext = true;

        private SingleElementIterator(Data data) {
            this.data = data;
        }

        @Override
        public boolean hasNext() {
            return hasNext;
        }

        @Override
        public Data next() {
            if (!hasNext) {
                throw new NoSuchElementException();
            }
            hasNext = false;
            return data;
        }

        @Override
        public void remove() {
        }
    }

    private static class EmptyIterator implements Iterator<Data> {

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public Data next() {
            throw new NoSuchElementException();
        }

        @Override
        public void remove() {
        }
    }
}
