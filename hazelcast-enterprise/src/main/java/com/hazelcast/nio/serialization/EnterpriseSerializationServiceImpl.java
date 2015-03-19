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

package com.hazelcast.nio.serialization;

import com.hazelcast.core.ManagedContext;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.nio.EnterpriseBufferObjectDataInput;
import com.hazelcast.nio.EnterpriseBufferObjectDataOutput;
import com.hazelcast.nio.EnterpriseObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Collection;
import java.util.Map;

import static com.hazelcast.nio.UnsafeHelper.BYTE_ARRAY_BASE_OFFSET;
import static com.hazelcast.nio.serialization.NativeMemoryData.NATIVE_HEADER_OVERHEAD;
import static com.hazelcast.nio.serialization.NativeMemoryData.SIZE_OFFSET;
import static com.hazelcast.nio.serialization.NativeMemoryData.TYPE_OFFSET;

public final class EnterpriseSerializationServiceImpl extends SerializationServiceImpl
        implements EnterpriseSerializationService {

    private final MemoryManager memoryManager;

    public EnterpriseSerializationServiceImpl(InputOutputFactory inputOutputFactory, int version,
            ClassLoader classLoader, Map<Integer, ? extends DataSerializableFactory> dataSerializableFactories,
            Map<Integer, ? extends PortableFactory> portableFactories, Collection<ClassDefinition> classDefinitions,
            boolean checkClassDefErrors, ManagedContext managedContext, PartitioningStrategy partitionStrategy,
            int initialOutputBufferSize, boolean enableCompression, boolean enableSharedObject,
            MemoryManager memoryManager) {

        super(inputOutputFactory, version, classLoader, dataSerializableFactories, portableFactories, classDefinitions,
                checkClassDefErrors, managedContext, partitionStrategy, initialOutputBufferSize, enableCompression,
                enableSharedObject);

        this.memoryManager = memoryManager;
    }

    @Override
    public Data toData(Object obj, DataType type) {
        return toData(obj, type, globalPartitioningStrategy);
    }

    @Override
    public Data toData(Object obj, DataType type, PartitioningStrategy strategy) {
        if (obj instanceof Data) {
            return convertData((Data) obj, type);
        }
        if (type == DataType.HEAP) {
            return super.toData(obj, strategy);
        }
        if (type == DataType.NATIVE) {
            return toNativeData(obj, strategy);
        }
        throw new IllegalArgumentException("Unknown data type: " + type);
    }

    private Data toNativeData(Object obj, PartitioningStrategy strategy) {
        if (obj == null) {
            return null;
        }

        if (memoryManager == null) {
            throw new IllegalArgumentException("MemoryManager is required!");
        }

        EnterpriseBufferObjectDataOutput out = (EnterpriseBufferObjectDataOutput) pop();
        try {
            SerializerAdapter serializer = serializerFor(obj.getClass());

            out.writeInt(serializer.getTypeId(), ByteOrder.BIG_ENDIAN);

            int partitionHash = calculatePartitionHash(obj, strategy);
            boolean hasPartitionHash = partitionHash != 0;
            out.writeBoolean(hasPartitionHash);

            serializer.write(out, obj);

            if (hasPartitionHash) {
                out.writeInt(partitionHash, ByteOrder.BIG_ENDIAN);
            }

            int size = out.position();
            int memSize = size + NATIVE_HEADER_OVERHEAD;
            long address = memoryManager.allocate(memSize);
            assert address != MemoryManager.NULL_ADDRESS : "Illegal memory access: " + address;

            NativeMemoryData data = new NativeMemoryData(address, memSize);
            data.writeInt(SIZE_OFFSET, size);
            out.copyToMemoryBlock(data, TYPE_OFFSET, size);
            return data;
        } catch (Throwable e) {
            throw handleException(e);
        } finally {
            push(out);
        }
    }

    @Override
    protected void writeDataInternal(ObjectDataOutput out, Data data) throws IOException {
        try {
            if (data instanceof NativeMemoryData && out instanceof EnterpriseBufferObjectDataOutput) {
                EnterpriseBufferObjectDataOutput bufferOut = (EnterpriseBufferObjectDataOutput) out;
                NativeMemoryData nativeMemoryData = (NativeMemoryData) data;
                bufferOut.writeInt(nativeMemoryData.totalSize());
                bufferOut.copyFromMemoryBlock(nativeMemoryData, NativeMemoryData.TYPE_OFFSET, data.totalSize());
            } else {
                out.writeByteArray(data.toByteArray());
            }
        } catch (Throwable e) {
            throw handleException(e);
        }
    }

    @Override
    public Data readData(EnterpriseObjectDataInput in, DataType type) {
        return readDataInternal(in, type, false);
    }

    @Override
    public Data tryReadData(EnterpriseObjectDataInput in, DataType type) {
        return readDataInternal(in, type, true);
    }

    private Data readDataInternal(EnterpriseObjectDataInput in, DataType type, boolean readToHeapOnOOME) {
        if (type == DataType.HEAP) {
            return super.readData(in);
        }

        if (memoryManager == null) {
            throw new HazelcastSerializationException("MemoryManager is required!");
        }

        try {
            boolean isNull = in.readBoolean();
            if (isNull) {
                return null;
            }

            int size = in.readInt();
            if (size == 0) {
                return new DefaultData(null);
            }

            return readNativeData(in, size, readToHeapOnOOME);
        } catch (Throwable e) {
            throw handleException(e);
        }
    }

    private Data readNativeData(EnterpriseObjectDataInput in, int size,  boolean readToHeapOnOOME) throws IOException {
        try {
            int memSize = size + NATIVE_HEADER_OVERHEAD;
            NativeMemoryData data = allocateNativeData(in, memSize, size, !readToHeapOnOOME);
            data.writeInt(SIZE_OFFSET, size);

            if (in instanceof EnterpriseBufferObjectDataInput) {
                EnterpriseBufferObjectDataInput bufferIn = (EnterpriseBufferObjectDataInput) in;
                bufferIn.copyToMemoryBlock(data, TYPE_OFFSET, size);
            } else {
                byte[] bytes = new byte[size];
                in.readFully(bytes);
                data.copyFrom(TYPE_OFFSET, bytes, BYTE_ARRAY_BASE_OFFSET, size);
            }
            return data;

        } catch (NativeOutOfMemoryError e) {
            if (readToHeapOnOOME) {
                byte[] bytes = new byte[size];
                in.readFully(bytes);
                return new DefaultData(bytes);
            } else {
                throw e;
            }
        }
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings("SR_NOT_CHECKED")
    private NativeMemoryData allocateNativeData(EnterpriseObjectDataInput in, int memSize, int size, boolean skipBytes)
            throws IOException {

        if (memoryManager == null) {
            throw new HazelcastSerializationException("MemoryManager is required!");
        }

        try {
            long address = memoryManager.allocate(memSize);
            return new NativeMemoryData(address, memSize);
        } catch (NativeOutOfMemoryError e) {
            if (skipBytes) {
                in.skipBytes(size);
            }
            throw e;
        }
    }

    public Data convertData(Data data, DataType type) {
        if (data == null) {
            return null;
        }
        switch (type) {
            case NATIVE:
                if (data instanceof DefaultData) {
                    if (memoryManager == null) {
                        throw new HazelcastSerializationException("MemoryManager is required!");
                    }

                    int size = data.totalSize();
                    int memSize = size + NATIVE_HEADER_OVERHEAD;

                    long address = memoryManager.allocate(memSize);
                    NativeMemoryData nativeData = new NativeMemoryData(address, memSize);
                    nativeData.writeInt(SIZE_OFFSET, size);
                    nativeData.copyFrom(TYPE_OFFSET, data.toByteArray(), BYTE_ARRAY_BASE_OFFSET, size);

                    return nativeData;
                }
                break;

            case HEAP:
                if (data instanceof NativeMemoryData) {
                    return new DefaultData(data.toByteArray());
                }
                break;

            default:
                throw new IllegalArgumentException();
        }
        return data;
    }

    public void disposeData(Data data) {
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
    public MemoryManager getMemoryManager() {
        return memoryManager;
    }

    public void destroy() {
        super.destroy();
        if (memoryManager != null) {
            memoryManager.destroy();
        }
    }
}
