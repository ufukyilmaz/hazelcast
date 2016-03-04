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
import com.hazelcast.memory.JvmMemoryManager;
import com.hazelcast.nio.EnterpriseBufferObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.PortableFactory;

import java.nio.ByteOrder;
import java.util.Map;

import static com.hazelcast.internal.memory.HeapMemoryAccessor.ARRAY_BYTE_BASE_OFFSET;
import static com.hazelcast.internal.serialization.impl.NativeMemoryData.NATIVE_MEMORY_DATA_OVERHEAD;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.handleException;

public final class EnterpriseSerializationServiceV1 extends SerializationServiceV1 implements EnterpriseSerializationService {
    private final boolean allowSerializeOffHeap;

    private final JvmMemoryManager memoryManager;
    private final ThreadLocal<JvmMemoryManager> memoryManagerThreadLocal = new ThreadLocal<JvmMemoryManager>();

    public EnterpriseSerializationServiceV1(InputOutputFactory inputOutputFactory, byte version, int portableVersion,
                                            ClassLoader classLoader, Map<Integer, ? extends DataSerializableFactory> dataSerializableFactories,
                                            Map<Integer, ? extends PortableFactory> portableFactories, ManagedContext managedContext,
                                            PartitioningStrategy partitionStrategy, int initialOutputBufferSize, BufferPoolFactory bufferPoolFactory,
                                            JvmMemoryManager memoryManager, boolean enableCompression, boolean enableSharedObject, boolean allowSerializeOffHeap) {
        super(inputOutputFactory, version, portableVersion, classLoader, dataSerializableFactories, portableFactories,
                managedContext, partitionStrategy, initialOutputBufferSize, bufferPoolFactory, enableCompression,
                enableSharedObject);

        this.memoryManager = memoryManager;
        this.allowSerializeOffHeap = allowSerializeOffHeap;
    }

    @Override
    public JvmMemoryManager getMemoryManagerToUse() {
        JvmMemoryManager mm = memoryManagerThreadLocal.get();
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
    public Data toNativeData(Object obj, JvmMemoryManager memoryManager) {
        return toDataInternal(obj, DataType.NATIVE, globalPartitioningStrategy, memoryManager);
    }

    @Override
    public Data toData(Object obj, DataType type, PartitioningStrategy strategy) {
        return toDataInternal(obj, type, strategy, getMemoryManagerToUse());
    }

    private Data toDataInternal(Object obj, DataType type, PartitioningStrategy strategy, JvmMemoryManager memoryManager) {
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

    private Data toNativeDataInternal(Object obj, PartitioningStrategy strategy, JvmMemoryManager memoryManager) {
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
            int memSize = size + NATIVE_MEMORY_DATA_OVERHEAD;
            long address = memoryManager.allocate(memSize);
            assert address != JvmMemoryManager.NULL_ADDRESS : "Illegal memory access: " + address;

            NativeMemoryData data = new NativeMemoryData(address, memSize);
            data.writeInt(NativeMemoryData.SIZE_OFFSET, size);
            out.copyToMemoryBlock(data, NATIVE_MEMORY_DATA_OVERHEAD, size);
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
    public Data convertToNativeData(Data data, JvmMemoryManager memoryManager) {
        return convertDataInternal(data, DataType.NATIVE, memoryManager);
    }

    private Data convertDataInternal(Data data, DataType type, JvmMemoryManager memoryManager) {
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
                    int memSize = size + NATIVE_MEMORY_DATA_OVERHEAD;

                    long address = memoryManager.allocate(memSize);
                    NativeMemoryData nativeData = new NativeMemoryData(address, memSize);
                    nativeData.writeInt(NativeMemoryData.SIZE_OFFSET, size);
                    nativeData.copyFrom(NATIVE_MEMORY_DATA_OVERHEAD, data.toByteArray(), ARRAY_BYTE_BASE_OFFSET, size);

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
    public void disposeData(Data data, JvmMemoryManager memoryManager) {
        disposeDataInternal(data, memoryManager);
    }

    private void disposeDataInternal(Data data, JvmMemoryManager memoryManager) {
        if (data instanceof NativeMemoryData) {
            if (memoryManager == null) {
                throw new HazelcastSerializationException("MemoryManager is required!");
            }
            NativeMemoryData memoryBlock = (NativeMemoryData) data;
            if (memoryBlock.address() != JvmMemoryManager.NULL_ADDRESS) {
                memoryManager.free(memoryBlock.address(), memoryBlock.size());
                memoryBlock.reset(JvmMemoryManager.NULL_ADDRESS);
            }
        } else {
            super.disposeData(data);
        }
    }

    @Override
    public <T> T toObject(Object data, JvmMemoryManager memoryManager) {
        try {
            memoryManagerThreadLocal.set(memoryManager);
            return super.toObject(data);
        } finally {
            memoryManagerThreadLocal.remove();
        }
    }

    @Override
    public JvmMemoryManager getMemoryManager() {
        return memoryManager;
    }

    @Override
    public void destroy() {
        super.destroy();
        if (memoryManager != null) {
            memoryManager.dispose();
        }
    }

    @Override
    public OffHeapDataInput createOffHeapObjectDataInput(long dataAddress, long dataSize) {
        if (!this.allowSerializeOffHeap) {
            throw new UnsupportedOperationException("Unsupported for heap de-serializer");
        }

        return ((OffHeapInputFactory) inputOutputFactory).createInput(dataAddress, dataSize, this);
    }

    @Override
    public OffHeapDataOutput createOffHeapObjectDataOutput(long bufferSize) {
        if (!this.allowSerializeOffHeap) {
            throw new UnsupportedOperationException("Unsupported for heap serializer");
        }

        return ((OffHeapOutputFactory) inputOutputFactory).createOutput(bufferSize, this);
    }
}
