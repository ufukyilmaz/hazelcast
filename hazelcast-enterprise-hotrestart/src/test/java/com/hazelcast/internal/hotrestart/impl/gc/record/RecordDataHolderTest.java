package com.hazelcast.internal.hotrestart.impl.gc.record;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.ByteBuffer;

import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RecordDataHolderTest {

    private final RecordDataHolder holder = new RecordDataHolder();
    private final int requestedSize = 1023;

    @Test
    public void getKeyBuffer_getsKeyBufferWithEnoughRemaining() {
        // When
        final ByteBuffer buf = holder.getKeyBuffer(requestedSize);

        // Then
        assertTrue(buf.remaining() >= requestedSize);
    }

    @Test
    public void getValueBuffer_getsKeyBufferWithEnoughRemaining() {
        // When
        final ByteBuffer buf = holder.getValueBuffer(requestedSize);

        // Then
        assertTrue(buf.remaining() >= requestedSize);
    }

    @Test
    public void clear_clearsBothBuffers() {
        // Given
        final ByteBuffer keyBuf = holder.getKeyBuffer(requestedSize);
        final ByteBuffer valBuf = holder.getValueBuffer(requestedSize);
        final int keyRemainingInitially = keyBuf.remaining();
        final int valRemainingInitially = keyBuf.remaining();
        keyBuf.putInt(10);
        valBuf.putInt(10);

        // When
        holder.clear();

        // Then
        assertEquals(keyRemainingInitially, keyBuf.remaining());
        assertEquals(valRemainingInitially, valBuf.remaining());
    }

    @Test
    public void flip_flipsBothBuffers() {
        // Given
        final ByteBuffer keyBuf = holder.getKeyBuffer(requestedSize);
        final ByteBuffer valBuf = holder.getValueBuffer(requestedSize);
        final int keyRemainingInitially = keyBuf.remaining();
        final int valRemainingInitially = keyBuf.remaining();
        keyBuf.putInt(10);
        valBuf.putInt(10);

        // When
        holder.flip();

        // Then
        assertEquals(INT_SIZE_IN_BYTES, keyBuf.remaining());
        assertEquals(INT_SIZE_IN_BYTES, valBuf.remaining());
    }
}
