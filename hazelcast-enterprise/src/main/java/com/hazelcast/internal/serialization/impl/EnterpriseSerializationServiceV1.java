package com.hazelcast.internal.serialization.impl;

import com.hazelcast.partition.PartitioningStrategy;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.serialization.impl.bufferpool.BufferPool;
import com.hazelcast.memory.HazelcastMemoryManager;
import com.hazelcast.nio.EnterpriseBufferObjectDataOutput;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.nio.serialization.HazelcastSerializationException;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Map;

import static com.hazelcast.internal.memory.HeapMemoryAccessor.ARRAY_BYTE_BASE_OFFSET;
import static com.hazelcast.internal.serialization.impl.EnterpriseDataSerializableHeader.isIdentifiedDataSerializable;
import static com.hazelcast.internal.serialization.impl.EnterpriseDataSerializableHeader.isVersioned;
import static com.hazelcast.internal.serialization.impl.NativeMemoryData.NATIVE_MEMORY_DATA_OVERHEAD;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.createSerializerAdapter;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.handleException;
import static com.hazelcast.util.Preconditions.checkNotNull;

public final class EnterpriseSerializationServiceV1 extends SerializationServiceV1 implements EnterpriseSerializationService {

    private static final int FACTORY_AND_CLASS_ID_BYTE_LENGTH = 8;
    private static final int VERSION_BYTE_LENGTH = 2;

    private final HazelcastMemoryManager memoryManager;
    private final ThreadLocal<MemoryAllocator> mallocThreadLocal = new ThreadLocal<MemoryAllocator>();

    EnterpriseSerializationServiceV1(Builder builder) {
        super(builder);

        this.memoryManager = builder.memoryManager;
        overrideConstantSerializers(builder.getClassLoader(), builder.clusterVersionAware, builder.getDataSerializableFactories(),
                builder.versionedSerializationEnabled);
    }

    private void overrideConstantSerializers(ClassLoader classLoader, EnterpriseClusterVersionAware clusterVersionAware,
                                             Map<Integer, ? extends DataSerializableFactory> dataSerializableFactories,
                                             boolean versionedSerializationEnabled) {
        // the EE client does not use the versioned serialization whereas the EE server does
        if (versionedSerializationEnabled) {
            clusterVersionAware = checkNotNull(clusterVersionAware, "ClusterVersionAware can't be null");
            this.dataSerializerAdapter = createSerializerAdapter(
                    new EnterpriseDataSerializableSerializer(dataSerializableFactories, classLoader, clusterVersionAware), this);
            registerConstant(DataSerializable.class, dataSerializerAdapter);
        }
    }

    @Override
    public MemoryAllocator getCurrentMemoryAllocator() {
        MemoryAllocator malloc = mallocThreadLocal.get();
        return malloc != null ? malloc : memoryManager;
    }

    @Override
    public <B extends Data> B toData(Object obj, DataType type) {
        return toDataInternal(obj, type, globalPartitioningStrategy, getCurrentMemoryAllocator());
    }

    @Override
    public <B extends Data> B toNativeData(Object obj, MemoryAllocator malloc) {
        return toDataInternal(obj, DataType.NATIVE, globalPartitioningStrategy, malloc);
    }

    @Override
    public <B extends Data> B toData(Object obj, DataType type, PartitioningStrategy strategy) {
        return toDataInternal(obj, type, strategy, getCurrentMemoryAllocator());
    }

    private <B extends Data> B toDataInternal(Object obj, DataType type, PartitioningStrategy strategy, MemoryAllocator malloc) {
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
            return toNativeDataInternal(obj, strategy, malloc);
        }
        throw new IllegalArgumentException("Unknown data type: " + type);
    }

    private <B extends Data> B toNativeDataInternal(Object obj, PartitioningStrategy strategy, MemoryAllocator malloc) {
        if (obj == null) {
            return null;
        }

        if (malloc == null) {
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
            long address = malloc.allocate(memSize);
            assert address != HazelcastMemoryManager.NULL_ADDRESS : "Illegal memory access: " + address;

            NativeMemoryData data = new NativeMemoryData(address, memSize);
            data.writeInt(NativeMemoryData.SIZE_OFFSET, size);
            out.copyToMemoryBlock(data, NATIVE_MEMORY_DATA_OVERHEAD, size);
            return (B) data;
        } catch (Throwable e) {
            throw handleException(e);
        } finally {
            pool.returnOutputBuffer(out);
        }
    }

    @Override
    public <B extends Data> B convertData(Data data, DataType type) {
        return convertDataInternal(data, type, getCurrentMemoryAllocator());
    }

    @Override
    public <B extends Data> B convertToNativeData(Data data, MemoryAllocator malloc) {
        return convertDataInternal(data, DataType.NATIVE, malloc);
    }

    private static <B extends Data> B convertDataInternal(Data data, DataType type, MemoryAllocator malloc) {
        if (data == null) {
            return null;
        }
        switch (type) {
            case NATIVE:
                if (data instanceof HeapData) {
                    if (malloc == null) {
                        throw new HazelcastSerializationException("MemoryManager is required!");
                    }

                    int size = data.totalSize();
                    int memSize = size + NATIVE_MEMORY_DATA_OVERHEAD;

                    long address = malloc.allocate(memSize);
                    NativeMemoryData nativeData = new NativeMemoryData(address, memSize);
                    nativeData.writeInt(NativeMemoryData.SIZE_OFFSET, size);
                    nativeData.copyFrom(NATIVE_MEMORY_DATA_OVERHEAD, data.toByteArray(), ARRAY_BYTE_BASE_OFFSET, size);

                    return (B) nativeData;
                }
                break;

            case HEAP:
                if (data instanceof NativeMemoryData) {
                    return (B) new HeapData(data.toByteArray());
                }
                break;

            default:
                throw new IllegalArgumentException();
        }
        return (B) data;
    }

    @Override
    public void disposeData(Data data) {
        disposeDataInternal(data, getCurrentMemoryAllocator());
    }

    @Override
    public void disposeData(Data data, MemoryAllocator memoryAllocator) {
        disposeDataInternal(data, memoryAllocator);
    }

    private void disposeDataInternal(Data data, MemoryAllocator malloc) {
        if (data instanceof NativeMemoryData) {
            if (malloc == null) {
                throw new HazelcastSerializationException("MemoryManager is required!");
            }
            NativeMemoryData memoryBlock = (NativeMemoryData) data;
            if (memoryBlock.address() != HazelcastMemoryManager.NULL_ADDRESS) {
                malloc.free(memoryBlock.address(), memoryBlock.size());
                memoryBlock.reset(HazelcastMemoryManager.NULL_ADDRESS);
            }
        } else {
            super.disposeData(data);
        }
    }

    @Override
    public <T> T toObject(Object data, MemoryAllocator memoryAllocator) {
        try {
            mallocThreadLocal.set(memoryAllocator);
            return super.toObject(data);
        } finally {
            mallocThreadLocal.remove();
        }
    }

    @Override
    public HazelcastMemoryManager getMemoryManager() {
        return memoryManager;
    }

    @Override
    public void dispose() {
        super.dispose();
        if (memoryManager != null) {
            memoryManager.dispose();
        }
    }

    @Override
    public ObjectDataInput initDataSerializableInputAndSkipTheHeader(Data data) throws IOException {
        ObjectDataInput input = createObjectDataInput(data);
        byte header = input.readByte();
        if (isIdentifiedDataSerializable(header)) {
            skipBytesSafely(input, FACTORY_AND_CLASS_ID_BYTE_LENGTH);
        } else {
            // read class-name
            input.readUTF();
        }

        if (isVersioned(header)) {
            skipBytesSafely(input, VERSION_BYTE_LENGTH);
        }
        return input;
    }

    private void skipBytesSafely(ObjectDataInput input, int count) throws IOException {
        if (input.skipBytes(count) != count) {
            throw new HazelcastSerializationException("Malformed serialization format");
        }
    }

    public static Builder enterpriseBuilder() {
        return new Builder();
    }

    public static final class Builder extends SerializationServiceV1.AbstractBuilder<Builder> {
        private HazelcastMemoryManager memoryManager;
        private EnterpriseClusterVersionAware clusterVersionAware;
        private boolean versionedSerializationEnabled;

        private Builder() {
        }

        public Builder withMemoryManager(HazelcastMemoryManager memoryManager) {
            this.memoryManager = memoryManager;
            return self();
        }

        public Builder withClusterVersionAware(EnterpriseClusterVersionAware clusterVersionAware) {
            this.clusterVersionAware = clusterVersionAware;
            return self();
        }

        public Builder withVersionedSerializationEnabled(boolean versionedSerializationEnabled) {
            this.versionedSerializationEnabled = versionedSerializationEnabled;
            return self();
        }

        @Override
        protected Builder self() {
            return this;
        }

        public EnterpriseSerializationServiceV1 build() {
            return new EnterpriseSerializationServiceV1(this);
        }
    }

}
