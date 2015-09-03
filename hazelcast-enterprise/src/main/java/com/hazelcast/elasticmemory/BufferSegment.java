package com.hazelcast.elasticmemory;

import com.hazelcast.elasticmemory.error.BufferSegmentClosedError;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.EmptyStatement;
import com.hazelcast.util.QuickMath;

import java.io.Closeable;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.lang.String.format;

public class BufferSegment implements Closeable {

    private static final ILogger LOGGER = Logger.getLogger(BufferSegment.class.getName());

    private static int id;

    private final Lock lock = new ReentrantLock();
    private final Queue<ByteBuffer> bufferPool = new ConcurrentLinkedQueue<ByteBuffer>();
    private final int chunkSize;

    private IntegerQueue chunks;

    // used only for duplicates; no read, no write
    private volatile ByteBuffer mainBuffer;

    public BufferSegment(int capacity, int chunkSize) {
        assertTrue((capacity % chunkSize == 0), format("Segment size[%d] must be multitude of chunk size[%d]!",
                capacity, chunkSize));

        int index = nextId();
        int chunkCount = capacity / chunkSize;
        if (LOGGER.isFinestEnabled()) {
            LOGGER.finest("BufferSegment[" + index + "] starting with chunkCount=" + chunkCount);
        }

        this.chunkSize = chunkSize;
        this.chunks = new IntegerQueue(chunkCount);
        this.mainBuffer = ByteBuffer.allocateDirect(capacity);

        for (int i = 0; i < chunkCount; i++) {
            chunks.offer(i);
        }
        if (LOGGER.isFinestEnabled()) {
            LOGGER.finest("BufferSegment[" + index + "] started!");
        }
    }

    private static synchronized int nextId() {
        return id++;
    }

    public DataRefImpl put(Data data) {
        if (data == null) {
            return null;
        }
        byte[] value = data.toByteArray();
        if (value == null || value.length == 0) {
            // volatile write;
            return new DataRefImpl(null, 0);
        }

        int count = QuickMath.divideByAndCeilToInt(value.length, chunkSize);
        // volatile read
        ByteBuffer buffer = getBuffer();
        if (buffer == null) {
            throw new BufferSegmentClosedError();
        }
        // operation under lock
        int[] indexes = reserve(count);

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
        // volatile write
        return new DataRefImpl(indexes, value.length);
    }

    public Data get(DataRefImpl ref) {
        // volatile read
        if (!isEntryRefValid(ref)) {
            return null;
        }
        if (ref.isEmpty()) {
            return new HeapData(null);
        }

        byte[] value = new byte[ref.size()];
        int chunkCount = ref.getChunkCount();
        int offset = 0;
        // volatile read
        ByteBuffer buffer = getBuffer();
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

        // volatile read
        if (isEntryRefValid(ref)) {
            return new HeapData(value);
        }
        return null;
    }

    private ByteBuffer getBuffer() {
        // volatile read
        ByteBuffer mb = mainBuffer;
        if (mb != null) {
            ByteBuffer buff = bufferPool.poll();
            if (buff == null) {
                return mb.duplicate();
            }
            return buff;
        }
        return null;
    }

    public void remove(DataRefImpl ref) {
        // volatile read
        if (!isEntryRefValid(ref)) {
            return;
        }
        // volatile write
        ref.invalidate();
        int chunkCount = ref.getChunkCount();
        if (chunkCount > 0) {
            int[] indexes = new int[chunkCount];
            for (int i = 0; i < chunkCount; i++) {
                indexes[i] = ref.getChunk(i);
            }
            assertTrue(release(indexes), "Could not offer released indexes! Error in queue...");
        }
    }

    private boolean isEntryRefValid(DataRefImpl ref) {
        //isValid() volatile read
        return ref != null && ref.isValid();
    }

    private static void assertTrue(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    public void close() {
        ByteBuffer buff = mainBuffer;
        // volatile write
        mainBuffer = null;
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
                EmptyStatement.ignore(ignored);
            }
        }
    }

    private int[] reserve(int count) {
        lock.lock();
        try {
            if (chunks == null) {
                throw new BufferSegmentClosedError();
            }
            int[] indexes = new int[count];
            return chunks.poll(indexes);
        } finally {
            lock.unlock();
        }
    }

    private boolean release(int[] indexes) {
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
