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

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.nio.EnterpriseBufferObjectDataInput;
import com.hazelcast.nio.EnterpriseBufferObjectDataOutput;
import com.hazelcast.nio.EnterpriseObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;

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

    protected SerializerAdapter createSerializerAdapter(Serializer serializer) {
        final SerializerAdapter s;
        if (serializer instanceof StreamSerializer) {
            s = new EnterpriseStreamSerializerAdapter(this, (StreamSerializer) serializer);
        } else if (serializer instanceof ByteArraySerializer) {
            s = new EnterpriseByteArraySerializerAdapter((ByteArraySerializer) serializer);
        } else {
            throw new IllegalArgumentException("Serializer must be instance of either "
                    + "StreamSerializer or ByteArraySerializer!");
        }
        return s;
    }

    @Override
    public Data toData(Object obj, DataType type) {
        return toData(obj, type, globalPartitioningStrategy);
    }

    @Override
    public Data toData(Object obj, DataType type, PartitioningStrategy strategy) {
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
            return toNativeData(obj, strategy);
        }
        throw new IllegalArgumentException("Unknown data type: " + type);
    }

    private Data toNativeData(Object obj, PartitioningStrategy strategy) {
        if (memoryManager == null) {
            throw new IllegalArgumentException("MemoryManager is required!");
        }

        int partitionHash = calculatePartitionHash(obj, strategy);
        try {
            final EnterpriseSerializerAdapter serializer = (EnterpriseSerializerAdapter) serializerFor(obj.getClass());
            if (serializer == null) {
                if (isActive()) {
                    throw new HazelcastSerializationException("There is no suitable serializer for " + obj.getClass());
                }
                throw new HazelcastInstanceNotActiveException();
            }
            return serializer.write(obj, memoryManager, partitionHash);
        } catch (Throwable e) {
            throw handleException(e);
        }
    }

    @Override
    protected void writeDataInternal(ObjectDataOutput out, Data data) throws IOException {
        if (data instanceof NativeMemoryData && out instanceof EnterpriseBufferObjectDataOutput) {
            EnterpriseBufferObjectDataOutput bufferOut = (EnterpriseBufferObjectDataOutput) out;
            NativeMemoryData nativeMemoryData = (NativeMemoryData) data;
            bufferOut.copyFromMemoryBlock(nativeMemoryData, NativeMemoryData.HEADER_LENGTH, data.dataSize());
        } else {
            out.write(data.getData());
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
            return readData(in);
        }

        if (memoryManager == null) {
            throw new HazelcastSerializationException("MemoryManager is required!");
        }

        try {
            boolean isNull = in.readBoolean();
            if (isNull) {
                return null;
            }

            int typeId = in.readInt();
            int partitionHash = in.readInt();
            byte[] header = readPortableHeader(in);

            int dataSize = in.readInt();
            if (dataSize > 0) {
                return readNativeData(in, typeId, partitionHash, dataSize, header, readToHeapOnOOME);
            }
            return new DefaultData(typeId, null, partitionHash, header);
        } catch (Throwable e) {
            throw handleException(e);
        }
    }

    private Data readNativeData(EnterpriseObjectDataInput in, int typeId, int partitionHash, int dataSize,
            byte[] header, boolean readToHeapOnOOME) throws IOException {

        int size = dataSize + NativeMemoryData.HEADER_LENGTH;
        if (header != null) {
            size += (INT_SIZE_IN_BYTES + header.length);
        }
        if (partitionHash != 0) {
            size += INT_SIZE_IN_BYTES;
        }

        MutableData mutableData = null;

        try {
            mutableData = allocateNativeData(in, dataSize, size, !readToHeapOnOOME);
        } catch (NativeOutOfMemoryError oome) {
            if (readToHeapOnOOME) {
                mutableData = new DefaultData();
            } else {
                throw oome;
            }
        }
        mutableData.setType(typeId);

        if (in instanceof EnterpriseBufferObjectDataInput && mutableData instanceof NativeMemoryData) {
            NativeMemoryData nativeMemoryData = (NativeMemoryData) mutableData;
            EnterpriseBufferObjectDataInput bufferInput = (EnterpriseBufferObjectDataInput) in;
            bufferInput.copyToMemoryBlock(nativeMemoryData, NativeMemoryData.HEADER_LENGTH, dataSize);
            nativeMemoryData.setDataSize(dataSize);
        } else {
            byte[] data = new byte[dataSize];
            in.readFully(data);
            mutableData.setData(data);
        }

        mutableData.setPartitionHash(partitionHash);
        mutableData.setHeader(header);
        return mutableData;
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings("SR_NOT_CHECKED")
    private NativeMemoryData allocateNativeData(EnterpriseObjectDataInput in, int dataSize, int size, boolean skipBytes)
            throws IOException {
        try {
            long address = memoryManager.allocate(size);
            return new NativeMemoryData(address, size);
        } catch (NativeOutOfMemoryError e) {
            if (skipBytes) {
                in.skipBytes(dataSize);
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
                    int size = data.dataSize() + NativeMemoryData.HEADER_LENGTH;
                    int partitionHash = data.hasPartitionHash() ? data.getPartitionHash() : 0;
                    if (partitionHash != 0) {
                        size += INT_SIZE_IN_BYTES;
                    }
                    long address = memoryManager.allocate(size);
                    NativeMemoryData bin = new NativeMemoryData(address, size);
                    bin.setType(data.getType());
                    bin.setData(data.getData());
                    bin.setPartitionHash(partitionHash);
                    return bin;
                }
                break;

            case HEAP:
                if (data instanceof NativeMemoryData) {
                    return new DefaultData(data.getType(), data.getData(), data.getPartitionHash());
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
                memoryBlock.setType(SerializationConstants.CONSTANT_TYPE_NULL);
                memoryBlock.setData(null);
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
