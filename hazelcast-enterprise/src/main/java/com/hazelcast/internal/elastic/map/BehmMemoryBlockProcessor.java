package com.hazelcast.internal.elastic.map;

import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.memory.MemoryBlockProcessor;
import com.hazelcast.internal.memory.MemoryBlock;
import com.hazelcast.internal.memory.MemoryBlockAccessor;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.internal.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;

/** Memory block processor for values of {@link BinaryElasticHashMap}.
 * @param <V> the type of the memory block. */
class BehmMemoryBlockProcessor<V extends MemoryBlock>
        implements MemoryBlockProcessor<V> {

    private final EnterpriseSerializationService serializationService;
    private final MemoryBlockAccessor<V> memoryBlockAccessor;
    private final MemoryAllocator malloc;

    BehmMemoryBlockProcessor(EnterpriseSerializationService serializationService,
                             MemoryBlockAccessor<V> memoryBlockAccessor, MemoryAllocator malloc) {
        this.serializationService = serializationService;
        this.memoryBlockAccessor = memoryBlockAccessor;
        this.malloc = malloc;
    }

    @Override
    public boolean isEqual(long address, V value) {
        return memoryBlockAccessor.isEqual(address, value);
    }

    @Override
    public boolean isEqual(long address1, long address2) {
        return memoryBlockAccessor.isEqual(address1, address2);
    }

    @Override
    public V read(long address) {
        return memoryBlockAccessor.read(address);
    }

    @Override
    public long dispose(long address) {
        return memoryBlockAccessor.dispose(address);
    }

    @Override
    public long dispose(V block) {
        return memoryBlockAccessor.dispose(block);
    }

    @Override
    public Data toData(Object obj, DataType dataType) {
        return serializationService.toData(obj, dataType);
    }

    @Override
    public Object toObject(Object data) {
        return serializationService.toObject(data);
    }

    @Override
    public Data convertData(Data data, DataType dataType) {
        return serializationService.convertData(data, dataType);
    }

    @Override
    public void disposeData(Data data) {
        serializationService.disposeData(data);
    }

    @Override
    public long allocate(long size) {
        return malloc.allocate(size);
    }

    @Override
    public void free(long address, long size) {
        malloc.free(address, size);
    }

    @Override
    public MemoryAllocator unwrapMemoryAllocator() {
        return malloc;
    }
}
