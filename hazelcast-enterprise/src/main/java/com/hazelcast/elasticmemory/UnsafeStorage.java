/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.elasticmemory;

import com.hazelcast.elasticmemory.error.BufferSegmentClosedError;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.UnsafeHelper;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DefaultData;
import com.hazelcast.internal.storage.Storage;
import com.hazelcast.util.QuickMath;
import sun.misc.Unsafe;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


class UnsafeStorage implements Storage<DataRefImpl> {

    private static final ILogger logger = Logger.getLogger(UnsafeStorage.class.getName());

    private final Lock lock = new ReentrantLock();
    private final int chunkSize;
    private final long address;
    private final Runnable cleaner;
    private IntegerQueue chunks;

    public UnsafeStorage(long capacity, int chunkSize) {
        super();
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be positive!");
        }
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("Chunk size must be positive!");
        }

        this.chunkSize = chunkSize;
        if (capacity % chunkSize != 0) {
            capacity = QuickMath.divideByAndCeilToLong(capacity, chunkSize) * chunkSize;
        }

        assertTrue((capacity % chunkSize == 0), "Storage size[" + capacity
                + "] must be multitude of chunk size[" + chunkSize + "]!");

        long cc = capacity / chunkSize;
        if (cc > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Chunk count limit exceeded; max: " + Integer.MAX_VALUE
                + ", required: " + cc + "! Please increment chunk size to a greater value than " + chunkSize + "KB.");
        }

        Unsafe unsafe = UnsafeHelper.UNSAFE;
        address = unsafe.allocateMemory(capacity);
        unsafe.setMemory(address, capacity, (byte) 0);
        cleaner = new Runnable() {
            public void run() {
                try {
                    UnsafeHelper.UNSAFE.freeMemory(address);
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
        };

        int chunkCount = (int) cc;
        chunks = new IntegerQueue(chunkCount);
        for (int i = 0; i < chunkCount; i++) {
            chunks.offer(i);
        }
        logger.finest("UnsafeStorage started!");
    }

    public DataRefImpl put(int hash, final Data data) {
        if (data == null) {
            return null;
        }
        final byte[] value = data.toByteArray();
        if (value == null || value.length == 0) {
            return new DataRefImpl(null, 0); // volatile write;
        }

        final int count = QuickMath.divideByAndCeilToInt(value.length, chunkSize);
        final int[] indexes = reserve(count);  // operation under lock
        Unsafe unsafe = UnsafeHelper.UNSAFE;
        int offset = 0;
        for (int i = 0; i < count; i++) {
            long pos = indexes[i] * (long) chunkSize;
            int len = Math.min(chunkSize, (value.length - offset));
            unsafe.copyMemory(value, UnsafeHelper.BYTE_ARRAY_BASE_OFFSET + offset, null, (address + pos), len);
            offset += len;
        }
        return new DataRefImpl(indexes, value.length); // volatile write
    }

    public Data get(int hash, final DataRefImpl ref) {
        if (!isEntryRefValid(ref)) {  // volatile read
            return null;
        }
        if (ref.isEmpty()) {
            return new DefaultData(null);
        }

        final byte[] value = new byte[ref.size()];
        final int chunkCount = ref.getChunkCount();
        int offset = 0;
        Unsafe unsafe = UnsafeHelper.UNSAFE;
        for (int i = 0; i < chunkCount; i++) {
            long pos = ref.getChunk(i) * (long) chunkSize;
            int len = Math.min(chunkSize, (ref.size() - offset));
            unsafe.copyMemory(null, (address + pos), value, UnsafeHelper.BYTE_ARRAY_BASE_OFFSET + offset, len);
            offset += len;
        }

        if (isEntryRefValid(ref)) { // volatile read
            return new DefaultData(value);
        }
        return null;
    }

    public void remove(int hash, final DataRefImpl ref) {
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

    public void destroy() {
        lock.lock();
        try {
            chunks = null;
        } finally {
            lock.unlock();
        }
        cleaner.run();
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
