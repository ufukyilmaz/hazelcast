package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.spi.hotrestart.RecordDataSink;

import java.nio.ByteBuffer;

import static com.hazelcast.util.QuickMath.nextPowerOfTwo;

/**
 * Implementation of {@link RecordDataSink}.
 */
public final class RecordDataHolder implements RecordDataSink {
    /** Initial size of the key/value byte buffers */
    @SuppressWarnings("checkstyle:magicnumber")
    public static final int INITIAL_BUFSIZE = 1 << 6;

    ByteBuffer keyBuffer = ByteBuffer.allocate(INITIAL_BUFSIZE);
    ByteBuffer valueBuffer = ByteBuffer.allocate(INITIAL_BUFSIZE);

    @Override public ByteBuffer getKeyBuffer(int keySize) {
        return keyBuffer = (ByteBuffer) ensureBufferCapacity(keyBuffer, keySize).clear();
    }
    @Override public ByteBuffer getValueBuffer(int valueSize) {
        return valueBuffer = (ByteBuffer) ensureBufferCapacity(valueBuffer, valueSize).clear();
    }

    boolean payloadSizeValid(Record r) {
        assert keyBuffer.remaining() + valueBuffer.remaining() == r.payloadSize() : String.format(
                "Expected record size %,d doesn't match key %,d + value %,d = %,d",
                r.payloadSize(), keyBuffer.remaining(), valueBuffer.remaining(),
                keyBuffer.remaining() + valueBuffer.remaining()
            );
        return true;
    }

    private static ByteBuffer ensureBufferCapacity(ByteBuffer buf, int capacity) {
        return buf.capacity() >= capacity ? buf : ByteBuffer.allocate(nextPowerOfTwo(capacity));
    }
}
