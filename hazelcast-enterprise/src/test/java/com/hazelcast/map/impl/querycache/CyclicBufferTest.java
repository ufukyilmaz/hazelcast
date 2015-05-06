package com.hazelcast.map.impl.querycache;

import com.hazelcast.map.impl.querycache.accumulator.CyclicBuffer;
import com.hazelcast.map.impl.querycache.accumulator.DefaultCyclicBuffer;
import com.hazelcast.map.impl.querycache.event.sequence.Sequenced;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.util.QuickMath.nextPowerOfTwo;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class CyclicBufferTest {

    @Test(expected = IllegalArgumentException.class)
    public void testBufferCapacity_whenZero() throws Exception {
        new DefaultCyclicBuffer(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBufferCapacity_whenNegative() throws Exception {
        new DefaultCyclicBuffer(-1);
    }

    @Test
    public void testBufferSize_whenEmpty() throws Exception {
        final int maxCapacity = nextPowerOfTwo(10);
        CyclicBuffer buffer = new DefaultCyclicBuffer(maxCapacity);

        assertEquals("item count should be = " + 0, 0, buffer.size());
    }

    @Test
    public void testBufferSize_whenAddedOneItem() throws Exception {
        final int maxCapacity = nextPowerOfTwo(10);
        final int itemCount = 1;

        CyclicBuffer buffer = new DefaultCyclicBuffer(maxCapacity);
        for (int i = 1; i <= itemCount; i++) {
            buffer.add(new TestSequenced(i));
        }

        assertEquals("item count should be = " + itemCount, itemCount, buffer.size());
    }

    @Test
    public void testBufferSize_whenFilledLessThanCapacity() throws Exception {
        final int maxCapacity = nextPowerOfTwo(10);
        final int itemCount = 4;

        CyclicBuffer buffer = new DefaultCyclicBuffer(maxCapacity);
        for (int i = 1; i <= itemCount; i++) {
            buffer.add(new TestSequenced(i));
        }

        assertEquals("item count should be = " + itemCount, itemCount, buffer.size());
    }

    @Test
    public void testBufferSize_whenFilledMoreThanCapacity() throws Exception {
        final int maxCapacity = nextPowerOfTwo(4);
        final int itemCount = 40;

        CyclicBuffer buffer = new DefaultCyclicBuffer(maxCapacity);
        for (int i = 1; i <= itemCount; i++) {
            buffer.add(new TestSequenced(i));
        }

        assertEquals("item count should be = " + maxCapacity, maxCapacity, buffer.size());
    }


    @Test
    public void testBufferRead_withSequence() throws Exception {
        final int maxCapacity = nextPowerOfTwo(10);
        final int itemCount = 4;
        CyclicBuffer buffer = new DefaultCyclicBuffer(maxCapacity);

        for (int i = 1; i <= itemCount; i++) {
            buffer.add(new TestSequenced(i));
        }

        long readFromSequence = 1L;
        do {
            Sequenced sequenced = buffer.get(readFromSequence);
            if (sequenced == null) {
                break;
            }
            if (readFromSequence + 1 > itemCount) {
                break;
            }
            readFromSequence++;
        } while (true);

        assertEquals("read count should be = " + readFromSequence, readFromSequence, itemCount);
    }

    @Test
    public void testSetHead_returnsTrue_whenSuppliedSequenceInBuffer() throws Exception {
        final int maxCapacity = nextPowerOfTwo(16);
        final int itemCount = 40;
        CyclicBuffer buffer = new DefaultCyclicBuffer(maxCapacity);

        for (int i = 1; i <= itemCount; i++) {
            buffer.add(new TestSequenced(i));
        }

        long suppliedSequence = 37;

        boolean setHead = buffer.setHead(suppliedSequence);

        assertTrue("setHead call should be successful", setHead);
    }

    @Test
    public void testSetHead_returnsFalse_whenSuppliedSequenceIsNotInBuffer() throws Exception {
        final int maxCapacity = nextPowerOfTwo(16);
        final int itemCount = 40;
        CyclicBuffer buffer = new DefaultCyclicBuffer(maxCapacity);

        for (int i = 1; i <= itemCount; i++) {
            buffer.add(new TestSequenced(i));
        }

        long suppliedSequence = 3;

        boolean setHead = buffer.setHead(suppliedSequence);

        assertFalse("setHead call should fail", setHead);
    }

    @Test
    public void testSetHead_changesBufferSize_whenSucceeded() throws Exception {
        final int itemCount = 40;
        final int maxCapacity = nextPowerOfTwo(16);
        CyclicBuffer buffer = new DefaultCyclicBuffer(maxCapacity);

        for (int i = 1; i <= itemCount; i++) {
            buffer.add(new TestSequenced(i));
        }
        long newSequence = 37;
        buffer.setHead(newSequence);

        assertEquals("buffer size should be affected from setting new sequence", itemCount - newSequence + 1, buffer.size());
    }

    @Test
    public void testSetHead_doesNotChangeBufferSize_whenFailed() throws Exception {
        final int maxCapacity = nextPowerOfTwo(16);
        final int itemCount = 40;
        CyclicBuffer buffer = new DefaultCyclicBuffer(maxCapacity);

        for (int i = 1; i <= itemCount; i++) {
            buffer.add(new TestSequenced(i));
        }
        long newSequence = 3;
        buffer.setHead(newSequence);

        assertEquals("buffer size should not be affected", maxCapacity, buffer.size());
    }

    @Test
    public void test_size() throws Exception {
        final int maxCapacity = nextPowerOfTwo(10);
        CyclicBuffer buffer = new DefaultCyclicBuffer(maxCapacity);

        for (int i = 0; i < 1; i++) {
            buffer.add(new TestSequenced(i));
        }

        assertEquals(1, buffer.size());
    }

    private static class TestSequenced implements Sequenced {

        private long sequence;

        public TestSequenced(long seq) {
            this.sequence = seq;
        }

        @Override
        public long getSequence() {
            return sequence;
        }

        @Override
        public int getPartitionId() {
            return 0;
        }

        @Override
        public void setSequence(long sequence) {
            this.sequence = sequence;
        }

        @Override
        public String toString() {
            return "TestSequenced{" +
                    "sequence=" + sequence +
                    '}';
        }
    }
}
