package com.hazelcast.elasticmemory;

import com.hazelcast.elasticmemory.error.BufferSegmentClosedError;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DefaultData;
import com.hazelcast.util.QuickMath;

import java.io.Closeable;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class BufferSegment implements Closeable {

    private static final ILogger logger = Logger.getLogger(BufferSegment.class.getName());

    private static int ID = 0;

    private static synchronized int nextId() {
        return ID++;
    }

    private final Lock lock = new ReentrantLock();
    private final int chunkSize;
    private final Queue<ByteBuffer> bufferPool;
    private volatile ByteBuffer mainBuffer; // used only for duplicates; no read, no write
    private IntegerQueue chunks;

    public BufferSegment(int capacity, int chunkSize) {
        super();

        this.chunkSize = chunkSize;
        assertTrue((capacity % chunkSize == 0), "Segment size[" + capacity + "] must be multitude of chunk size[" + chunkSize + "]!");

        final int index = nextId();
        int chunkCount = capacity / chunkSize;
        logger.finest("BufferSegment[" + index + "] starting with chunkCount=" + chunkCount);

        chunks = new IntegerQueue(chunkCount);
        mainBuffer = ByteBuffer.allocateDirect(capacity);
        for (int i = 0; i < chunkCount; i++) {
            chunks.offer(i);
        }
        bufferPool = new ConcurrentLinkedQueue<ByteBuffer>();
        logger.finest("BufferSegment[" + index + "] started!");
    }

    public DataRefImpl put(final Data data) {
        if (data == null) {
            return null;
        }
        final byte[] value = data.toByteArray();
        if (value == null || value.length == 0) {
            return new DataRefImpl(null, 0); // volatile write;
        }

        final int count = QuickMath.divideByAndCeilToInt(value.length, chunkSize);
        final ByteBuffer buffer = getBuffer();   // volatile read
        if (buffer == null) {
            throw new BufferSegmentClosedError();
        }
        final int[] indexes = reserve(count);  // operation under lock

        try {
            int offset = 0;
            for (int i = 0; i < count; i++) {
                buffer.position(indexes[i] * chunkSize);
                int len = Math.min(chunkSize, (value.length - offset));
                buffer.put(value, offset, len);
                offset += len;
            }
        } finally {
            bufferPool.offer(buffer);
        }
        return new DataRefImpl(indexes, value.length); // volatile write
    }

    public Data get(final DataRefImpl ref) {
        if (!isEntryRefValid(ref)) {  // volatile read
            return null;
        }
        if (ref.isEmpty()) {
            return new DefaultData(null);
        }

        final byte[] value = new byte[ref.size()];
        final int chunkCount = ref.getChunkCount();
        int offset = 0;
        final ByteBuffer buffer = getBuffer();  // volatile read
        if (buffer == null) {
            throw new BufferSegmentClosedError();
        }
        try {
            for (int i = 0; i < chunkCount; i++) {
                buffer.position(ref.getChunk(i) * chunkSize);
                int len = Math.min(chunkSize, (ref.size() - offset));
                buffer.get(value, offset, len);
                offset += len;
            }
        } finally {
            bufferPool.offer(buffer);
        }

        if (isEntryRefValid(ref)) { // volatile read
            return new DefaultData(value);
        }
        return null;
    }

    private ByteBuffer getBuffer() { // volatile read
        final ByteBuffer mb = mainBuffer;
        if (mb != null) {
            ByteBuffer buff = bufferPool.poll();
            if (buff == null) {
                return mb.duplicate();
            }
            return buff;
        }
        return  null;
    }

    public void remove(final DataRefImpl ref) {
        if (!isEntryRefValid(ref)) { // volatile read
            return;
        }
        ref.invalidate(); // volatile write
        final int chunkCount = ref.getChunkCount();
        if (chunkCount > 0) {
            final int[] indexes = new int[chunkCount];
            for (int i = 0; i < chunkCount; i++) {
                indexes[i] = ref.getChunk(i);
            }
            assertTrue(release(indexes), "Could not offer released indexes! Error in queue...");
        }
    }

    private boolean isEntryRefValid(final DataRefImpl ref) {
        return ref != null && ref.isValid();  //isValid() volatile read
    }

    private static void assertTrue(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    public void close() {
        final ByteBuffer buff = mainBuffer;
        mainBuffer = null; // volatile write
        bufferPool.clear();
        lock.lock();
        try {
            chunks = null;
        } finally {
            lock.unlock();
        }

        if (buff != null) {
            try {
                Class<?> directBufferClass = Class.forName("sun.nio.ch.DirectBuffer");
                Method cleanerMethod = directBufferClass.getMethod("cleaner");
                Object cleaner = cleanerMethod.invoke(buff);
                Method cleanMethod = cleaner.getClass().getMethod("clean");
                cleanMethod.invoke(cleaner);
            } catch (Throwable ignored) {
            }
        }
    }

    private int[] reserve(final int count) {
        lock.lock();
        try {
            if (chunks == null) {
                throw new BufferSegmentClosedError();
            }
            final int[] indexes = new int[count];
            return chunks.poll(indexes);
        } finally {
            lock.unlock();
        }
    }

    private boolean release(final int[] indexes) {
        lock.lock();
        try {
            boolean b = true;
            for (int index : indexes) {
                b = chunks.offer(index) && b;
            }
            return b;
        } finally {
            lock.unlock();
        }
    }
}
