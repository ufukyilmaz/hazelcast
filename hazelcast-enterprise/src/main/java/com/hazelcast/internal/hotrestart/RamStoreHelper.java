package com.hazelcast.internal.hotrestart;

import com.hazelcast.internal.hidensity.HiDensityRecord;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.internal.memory.HazelcastMemoryManager;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.hotrestart.impl.KeyOnHeap;

import java.nio.ByteBuffer;

/**
 * Helper utility to provide common functionality for {@link RamStore}
 * implementations.
 */
public final class RamStoreHelper {

    private RamStoreHelper() {
    }

    /**
     * Helper {@link RamStore#copyEntry(KeyHandle, int, RecordDataSink)} method for heap based
     * {@link RamStore} implementations.
     */
    public static boolean copyEntry(KeyOnHeap keyHandle, Data value, int expectedSize, RecordDataSink sink) {
        byte[] keyBytes = keyHandle.bytes();
        byte[] valueBytes = value.toByteArray();

        if (valueBytes.length + keyBytes.length != expectedSize) {
            return false;
        }
        sink.getValueBuffer(valueBytes.length).put(valueBytes);
        sink.getKeyBuffer(keyBytes.length).put(keyBytes);
        return true;
    }

    /**
     * Helper {@link RamStore#copyEntry(KeyHandle, int, RecordDataSink)} method for off-heap based
     * {@link RamStore} implementations.
     */
    public static boolean copyEntry(KeyHandleOffHeap keyHandleOffHeap, NativeMemoryData key, HiDensityRecord record,
            int expectedSize, RecordDataSink sink) {

        if (keyHandleOffHeap.sequenceId() != record.getSequence()) {
            return false;
        }

        final int keySize = key.totalSize();
        final long valueAddress = record.getValueAddress();
        final NativeMemoryData value = new NativeMemoryData().reset(valueAddress);
        final int valueSize = value.totalSize();
        if (keySize + valueSize != expectedSize) {
            return false;
        }

        writeDataToBuffer(value, sink.getValueBuffer(valueSize));
        writeDataToBuffer(key, sink.getKeyBuffer(keySize));
        return true;
    }

    private static void writeDataToBuffer(NativeMemoryData data, ByteBuffer buffer) {
        if (buffer.hasArray()) {
            final byte[] bufferArray = buffer.array();
            final int position = buffer.position();
            final int length = data.totalSize();
            data.copyToByteArray(NativeMemoryData.NATIVE_MEMORY_DATA_OVERHEAD, bufferArray, position, length);
            buffer.position(position + length);
        } else {
            buffer.put(data.toByteArray());
        }
    }

    /**
     * Validates key handles address and returns a native key if validation is successful,
     * returns null otherwise.
     *
     * @param kh offheap key handle
     * @param memoryManager memory manager key handle address belongs to
     * @return native key
     */
    public static NativeMemoryData validateAndGetKey(KeyHandleOffHeap kh, HazelcastMemoryManager memoryManager) {
        int size = (int) memoryManager.validateAndGetUsableSize(kh.address());
        if (size < 0) {
            return null;
        }
        NativeMemoryData key = new NativeMemoryData(kh.address(), size);
        if (NativeMemoryData.NATIVE_MEMORY_DATA_OVERHEAD + key.totalSize() > size) {
            return null;
        }
        return key;
    }
}
