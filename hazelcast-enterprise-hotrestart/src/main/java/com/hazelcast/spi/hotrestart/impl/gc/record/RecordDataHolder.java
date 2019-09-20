package com.hazelcast.spi.hotrestart.impl.gc.record;

import com.hazelcast.spi.hotrestart.RecordDataSink;

import java.nio.ByteBuffer;

import static com.hazelcast.internal.util.QuickMath.nextPowerOfTwo;

/**
 * Implementation of {@link RecordDataSink}.
 */
public final class RecordDataHolder implements RecordDataSink {
    /** Initial size of the key/value byte buffers */
    @SuppressWarnings("checkstyle:magicnumber")
    public static final int INITIAL_BUFSIZE = 1 << 6;

    public ByteBuffer keyBuffer = ByteBuffer.allocate(INITIAL_BUFSIZE);
    public ByteBuffer valueBuffer = ByteBuffer.allocate(INITIAL_BUFSIZE);

    @Override
    public ByteBuffer getKeyBuffer(int keySize) {
        return keyBuffer = ensureBufferCapacity(keyBuffer, keySize);
    }

    @Override
    public ByteBuffer getValueBuffer(int valueSize) {
        return valueBuffer = ensureBufferCapacity(valueBuffer, valueSize);
    }

    public void clear() {
        keyBuffer.clear();
        valueBuffer.clear();
    }

    public void flip() {
        keyBuffer.flip();
        valueBuffer.flip();
    }

    private static ByteBuffer ensureBufferCapacity(ByteBuffer buf, int capacity) {
        return buf.capacity() >= capacity ? buf : ByteBuffer.allocate(nextPowerOfTwo(capacity));
    }
}
