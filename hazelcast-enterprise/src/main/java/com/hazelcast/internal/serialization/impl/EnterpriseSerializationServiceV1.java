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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.core.ManagedContext;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.internal.serialization.InputOutputFactory;
import com.hazelcast.internal.serialization.impl.bufferpool.BufferPool;
import com.hazelcast.internal.serialization.impl.bufferpool.BufferPoolFactory;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.nio.EnterpriseBufferObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.PortableFactory;

import java.nio.ByteOrder;
import java.util.Map;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.handleException;
import static com.hazelcast.nio.UnsafeHelper.BYTE_ARRAY_BASE_OFFSET;

public final class EnterpriseSerializationServiceV1 extends SerializationServiceV1 implements EnterpriseSerializationService {

    private final MemoryManager memoryManager;
    private final ThreadLocal<MemoryManager> memoryManagerThreadLocal = new ThreadLocal<MemoryManager>();

    public EnterpriseSerializationServiceV1(InputOutputFactory inputOutputFactory, byte version, int portableVersion,
            ClassLoader classLoader, Map<Integer, ? extends DataSerializableFactory> dataSerializableFactories,
            Map<Integer, ? extends PortableFactory> portableFactories, ManagedContext managedContext,
            PartitioningStrategy partitionStrategy, int initialOutputBufferSize, BufferPoolFactory bufferPoolFactory,
            MemoryManager memoryManager) {

        super(inputOutputFactory, version, portableVersion, classLoader, dataSerializableFactories, portableFactories,
                managedContext, partitionStrategy, initialOutputBufferSize, bufferPoolFactory);

        this.memoryManager = memoryManager;
    }

    @Override
    public MemoryManager getMemoryManagerToUse() {
        MemoryManager mm = memoryManagerThreadLocal.get();
        if (mm != null) {
            return mm;
        } else {
            return memoryManager;
        }
    }

    @Override
    public Data toData(Object obj, DataType type) {
        return toDataInternal(obj, type, globalPartitioningStrategy, getMemoryManagerToUse());
    }

    @Override
    public Data toNativeData(Object obj, MemoryManager memoryManager) {
        return toDataInternal(obj, DataType.NATIVE, globalPartitioningStrategy, memoryManager);
    }

    @Override
    public Data toData(Object obj, DataType type, PartitioningStrategy strategy) {
        return toDataInternal(obj, type, strategy, getMemoryManagerToUse());
    }

    private Data toDataInternal(Object obj, DataType type, PartitioningStrategy strategy, MemoryManager memoryManager) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof Data) {
            return convertData((Data) obj, type);
        }
        if (type == DataType.HEAP) {
            return super.toData(obj, strategy);
        }
        if (type == DataType.NATIVE) {
            return toNativeDataInternal(obj, strategy, memoryManager);
        }
        throw new IllegalArgumentException("Unknown data type: " + type);
    }

    private Data toNativeDataInternal(Object obj, PartitioningStrategy strategy, MemoryManager memoryManager) {
        if (obj == null) {
            return null;
        }

        if (memoryManager == null) {
            throw new IllegalArgumentException("MemoryManager is required!");
        }

        BufferPool pool = bufferPoolThreadLocal.get();
        EnterpriseBufferObjectDataOutput out = (EnterpriseBufferObjectDataOutput) pool.takeOutputBuffer();
        try {
            SerializerAdapter serializer = serializerFor(obj);

            int partitionHash = calculatePartitionHash(obj, strategy);
            out.writeInt(partitionHash, ByteOrder.BIG_ENDIAN);

            out.writeInt(serializer.getTypeId(), ByteOrder.BIG_ENDIAN);
            serializer.write(out, obj);

            int size = out.position();
            int memSize = size + NativeMemoryData.NATIVE_HEADER_OVERHEAD;
            long address = memoryManager.allocate(memSize);
            assert address != MemoryManager.NULL_ADDRESS : "Illegal memory access: " + address;

            NativeMemoryData data = new NativeMemoryData(address, memSize);
            data.writeInt(NativeMemoryData.SIZE_OFFSET, size);
            out.copyToMemoryBlock(data, NativeMemoryData.COPY_OFFSET, size);
            return data;
        } catch (Throwable e) {
            throw handleException(e);
        } finally {
            pool.returnOutputBuffer(out);
        }
    }

    @Override
    public Data convertData(Data data, DataType type) {
        return convertDataInternal(data, type, getMemoryManagerToUse());
    }

    @Override
    public Data convertToNativeData(Data data, MemoryManager memoryManager) {
        return convertDataInternal(data, DataType.NATIVE, memoryManager);
    }

    private Data convertDataInternal(Data data, DataType type, MemoryManager memoryManager) {
        if (data == null) {
            return null;
        }
        switch (type) {
            case NATIVE:
                if (data instanceof HeapData) {
                    if (memoryManager == null) {
                        throw new HazelcastSerializationException("MemoryManager is required!");
                    }

                    int size = data.totalSize();
                    int memSize = size + NativeMemoryData.NATIVE_HEADER_OVERHEAD;

                    long address = memoryManager.allocate(memSize);
                    NativeMemoryData nativeData = new NativeMemoryData(address, memSize);
                    nativeData.writeInt(NativeMemoryData.SIZE_OFFSET, size);
                    nativeData.copyFrom(NativeMemoryData.COPY_OFFSET, data.toByteArray(), BYTE_ARRAY_BASE_OFFSET, size);

                    return nativeData;
                }
                break;

            case HEAP:
                if (data instanceof NativeMemoryData) {
                    return new HeapData(data.toByteArray());
                }
                break;

            default:
                throw new IllegalArgumentException();
        }
        return data;
    }

    @Override
    public void disposeData(Data data) {
        disposeDataInternal(data, getMemoryManagerToUse());
    }

    @Override
    public void disposeData(Data data, MemoryManager memoryManager) {
        disposeDataInternal(data, memoryManager);
    }

    private void disposeDataInternal(Data data, MemoryManager memoryManager) {
        if (data instanceof NativeMemoryData) {
            if (memoryManager == null) {
                throw new HazelcastSerializationException("MemoryManager is required!");
            }
            NativeMemoryData memoryBlock = (NativeMemoryData) data;
            if (memoryBlock.address() != MemoryManager.NULL_ADDRESS) {
                memoryManager.free(memoryBlock.address(), memoryBlock.size());
                memoryBlock.reset(MemoryManager.NULL_ADDRESS);
            }
        } else {
            super.disposeData(data);
        }
    }

    @Override
    public <T> T toObject(Object data, MemoryManager memoryManager) {
        try {
            memoryManagerThreadLocal.set(memoryManager);
            return super.toObject(data);
        } finally {
            memoryManagerThreadLocal.remove();
        }
    }

    @Override
    public MemoryManager getMemoryManager() {
        return memoryManager;
    }

    @Override
    public void destroy() {
        super.destroy();
        if (memoryManager != null) {
            memoryManager.destroy();
        }
    }

}
