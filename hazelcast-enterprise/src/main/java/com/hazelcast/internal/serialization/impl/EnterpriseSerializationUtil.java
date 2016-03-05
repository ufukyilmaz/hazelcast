package com.hazelcast.internal.serialization.impl;

import com.hazelcast.memory.HazelcastMemoryManager;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.nio.EnterpriseBufferObjectDataInput;
import com.hazelcast.nio.EnterpriseObjectDataInput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;

import static com.hazelcast.internal.memory.HeapMemoryAccessor.ARRAY_BYTE_BASE_OFFSET;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.handleException;
import static com.hazelcast.nio.Bits.NULL_ARRAY_LENGTH;

/**
 * Enterprise Serialization utility methods
 */
public final class EnterpriseSerializationUtil {

    private EnterpriseSerializationUtil() {
    }

    public static Data readDataInternal(
            EnterpriseObjectDataInput in, DataType type, HazelcastMemoryManager memoryManager,
            boolean readToHeapOnOOME)
    throws IOException {
        if (type == DataType.HEAP) {
            return in.readData();
        }

        if (memoryManager == null) {
            throw new HazelcastSerializationException("MemoryManager is required!");
        }

        try {
            int size = in.readInt();
            if (size == NULL_ARRAY_LENGTH) {
                return null;
            }
            if (size == 0) {
                return new HeapData(null);
            }
            return readNativeData(in, memoryManager, size, readToHeapOnOOME);
        } catch (Throwable e) {
            throw handleException(e);
        }
    }

    public static Data readNativeData(EnterpriseObjectDataInput in, HazelcastMemoryManager memoryManager, int size,
                                      boolean readToHeapOnOOME) throws IOException {
        try {
            int memSize = size + NativeMemoryData.NATIVE_MEMORY_DATA_OVERHEAD;
            NativeMemoryData data = allocateNativeData(in, memoryManager, memSize, size, !readToHeapOnOOME);
            data.writeInt(NativeMemoryData.SIZE_OFFSET, size);

            if (in instanceof EnterpriseBufferObjectDataInput) {
                EnterpriseBufferObjectDataInput bufferIn = (EnterpriseBufferObjectDataInput) in;
                bufferIn.copyToMemoryBlock(data, NativeMemoryData.NATIVE_MEMORY_DATA_OVERHEAD, size);
            } else {
                byte[] bytes = new byte[size];
                in.readFully(bytes);
                data.copyFrom(NativeMemoryData.NATIVE_MEMORY_DATA_OVERHEAD, bytes, ARRAY_BYTE_BASE_OFFSET, size);
            }
            return data;
        } catch (NativeOutOfMemoryError e) {
            if (readToHeapOnOOME) {
                byte[] bytes = new byte[size];
                in.readFully(bytes);
                return new HeapData(bytes);
            } else {
                throw e;
            }
        }
    }

    @SuppressFBWarnings("SR_NOT_CHECKED")
    public static NativeMemoryData allocateNativeData(
            EnterpriseObjectDataInput in, HazelcastMemoryManager memoryManager, int memSize, int size,
            boolean skipBytesOnOome)
    throws IOException {
        if (memoryManager == null) {
            throw new HazelcastSerializationException("MemoryManager is required!");
        }
        try {
            long address = memoryManager.allocate(memSize);
            return new NativeMemoryData(address, memSize);
        } catch (NativeOutOfMemoryError e) {
            if (skipBytesOnOome) {
                in.skipBytes(size);
            }
            throw e;
        }
    }
}
