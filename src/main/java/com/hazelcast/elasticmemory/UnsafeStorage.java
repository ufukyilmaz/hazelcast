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
import com.hazelcast.elasticmemory.util.MemoryUnit;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.UnsafeHelper;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataAccessor;
import com.hazelcast.storage.Storage;
import sun.misc.Unsafe;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.hazelcast.elasticmemory.util.MathUtil.divideByAndCeil;

// Not used at the moment...
class UnsafeStorage implements Storage<DataRefImpl> {

    private static final ILogger logger = Logger.getLogger(UnsafeStorage.class.getName());

    private final Lock lock = new ReentrantLock();
    private final long totalSize;
    private final int chunkSize;
    private final long baseAddress;
    private IntegerQueue chunks;

    public UnsafeStorage(int totalSizeInMb, int chunkSizeInKb) {
        super();
        if (totalSizeInMb <= 0) {
            throw new IllegalArgumentException("Total size must be positive!");
        }
        if (chunkSizeInKb <= 0) {
            throw new IllegalArgumentException("Chunk size must be positive!");
        }

        long totalSizeInKb =  MemoryUnit.MEGABYTES.toKiloBytes(totalSizeInMb);
        if (totalSizeInKb % chunkSizeInKb != 0) {
            totalSizeInKb = divideByAndCeil(totalSizeInKb, (long) chunkSizeInKb) * chunkSizeInKb;
        }

        this.totalSize = MemoryUnit.KILOBYTES.toBytes(totalSizeInKb);
        this.chunkSize = (int) MemoryUnit.KILOBYTES.toBytes(chunkSizeInKb);

        assertTrue((totalSize % chunkSize == 0), "Storage size[" + totalSizeInKb
                + " MB] must be multitude of chunk size[" + chunkSizeInKb + " KB]!");

        long cc = totalSize / chunkSize;
        if (cc > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Chunk count limit exceeded; max: " + Integer.MAX_VALUE
                + ", required: " + cc + "! Please increment chunk size to a greater value than " + chunkSizeInKb + "KB.");
        }

        Unsafe unsafe = UnsafeHelper.UNSAFE;
        baseAddress = unsafe.allocateMemory(totalSize);
        unsafe.setMemory(baseAddress, totalSize, (byte) 0);

        int chunkCount = (int) cc;
        chunks = new IntegerQueue(chunkCount);
        for (int i = 0; i < chunkCount; i++) {
            chunks.offer(i);
        }
        logger.finest("UnsafeStorage started!");
    }

    public DataRefImpl put(int hash, final Data data) {
        final byte[] value = data != null ? data.getBuffer() : null;
        if (value == null || value.length == 0) {
            return DataRefImpl.EMPTY_DATA_REF;
        }

        final int count = divideByAndCeil(value.length, chunkSize);
        final int[] indexes = reserve(count);  // operation under lock
        Unsafe unsafe = UnsafeHelper.UNSAFE;
        int offset = 0;
        for (int i = 0; i < count; i++) {
            int pos = indexes[i] * chunkSize;
            int len = Math.min(chunkSize, (value.length - offset));
            unsafe.copyMemory(value, UnsafeHelper.BYTE_ARRAY_BASE_OFFSET + offset, null, (baseAddress + pos), len);
            offset += len;
        }
        return new DataRefImpl(data.getType(), indexes, value.length, data.getClassDefinition()); // volatile write
    }

    public Data get(int hash, final DataRefImpl ref) {
        if (!isEntryRefValid(ref)) {  // volatile read
            return null;
        }

        final byte[] value = new byte[ref.size()];
        final int chunkCount = ref.getChunkCount();
        int offset = 0;
        Unsafe unsafe = UnsafeHelper.UNSAFE;
        for (int i = 0; i < chunkCount; i++) {
            int pos = ref.getChunk(i) * chunkSize;
            int len = Math.min(chunkSize, (ref.size() - offset));
            unsafe.copyMemory(null, (baseAddress + pos), value, UnsafeHelper.BYTE_ARRAY_BASE_OFFSET + offset, len);
            offset += len;
        }

        if (isEntryRefValid(ref)) { // volatile read
            Data data = new Data(ref.getType(), value);
            DataAccessor.setCD(data, ref.getClassDefinition());
            return data;
        }
        return null;
    }

    public void remove(int hash, final DataRefImpl ref) {
        if (!isEntryRefValid(ref)) { // volatile read
            return;
        }
        ref.invalidate(); // volatile write
        final int chunkCount = ref.getChunkCount();
        final int[] indexes = new int[chunkCount];
        for (int i = 0; i < chunkCount; i++) {
            indexes[i] = ref.getChunk(i);
        }
        assertTrue(release(indexes), "Could not offer released indexes! Error in queue...");
    }

    private boolean isEntryRefValid(final DataRefImpl ref) {
        return ref != null && !ref.isEmpty() && ref.isValid();  //isValid() volatile read
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
        UnsafeHelper.UNSAFE.freeMemory(baseAddress);
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
