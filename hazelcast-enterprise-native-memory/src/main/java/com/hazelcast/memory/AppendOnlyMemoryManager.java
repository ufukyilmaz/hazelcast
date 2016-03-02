package com.hazelcast.memory;

import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry;
import com.hazelcast.internal.memory.GlobalMemoryAccessorType;
import com.hazelcast.internal.memory.impl.UnsafeMalloc;
import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.internal.util.counters.MwCounter;

import java.util.ArrayList;
import java.util.List;

public class AppendOnlyMemoryManager implements MemoryManager, Resetable {

    // We are using `STANDARD` memory accessor because we internally guarantee that every memory access is aligned
    private static final MemoryAccessor MEMORY_ACCESSOR = GlobalMemoryAccessorRegistry.getGlobalMemoryAccessor(GlobalMemoryAccessorType.STANDARD);

    private long pointer;

    private long usedMemorySize;

    private boolean isDestroyed;

    private int currentBlockIndex;

    private final int minBlockSize;

    private long acquiredMemorySize;

    private MemoryBlock currentMemoryBlock;

    private final NativeMemoryStats memoryStats;

    private final MemoryAllocator memoryAllocator;

    private final List<MemoryBlock> memoryBlockList = new ArrayList<MemoryBlock>();

    private final Counter sequenceGenerator = MwCounter.newMwCounter();

    public AppendOnlyMemoryManager(MemorySize cap, int minBlockSize) {
        this.minBlockSize = minBlockSize;
        this.memoryStats = new NativeMemoryStats(cap.bytes());
        this.memoryAllocator = new StandardMemoryManager(new UnsafeMalloc(), memoryStats);
    }

    private void allocateBlock(long desiredBlockSize) {
        long blockSize = Math.max(desiredBlockSize, minBlockSize);
        acquiredMemorySize += blockSize;

        if (currentBlockIndex < memoryBlockList.size() - 1) {
            currentBlockIndex++;
            MemoryBlock block = memoryBlockList.get(currentBlockIndex);

            if (blockSize > block.size) {
                while ((memoryBlockList.size() - 1 >= currentBlockIndex)) {
                    block = memoryBlockList.get(currentBlockIndex);

                    if (blockSize > block.size) {
                        memoryAllocator.free(block.address, block.size);
                        memoryBlockList.remove(currentBlockIndex);
                    } else {
                        currentMemoryBlock = block;
                        pointer = block.address;
                        return;
                    }
                }
            } else {
                currentMemoryBlock = block;
                pointer = block.address;
                return;
            }
        }

        pointer = memoryAllocator.allocate(blockSize);

        currentMemoryBlock = new MemoryBlock(pointer, blockSize);
        memoryBlockList.add(currentMemoryBlock);
        currentBlockIndex = memoryBlockList.size() - 1;
    }

    @Override
    public long allocate(long size) {
        usedMemorySize += size;

        if (requireNewMemoryBlock(pointer, size)) {
            allocateBlock(usedMemorySize - acquiredMemorySize);
        }

        long oldPointer = pointer;
        pointer += size;
        return oldPointer;
    }

    @Override
    public long reallocate(long address, long currentSize, long newSize) {
        if (pointer - currentSize != address) {
            throw new IllegalStateException(
                    "Can't re-allocate not last allocated memory currentSize=" + currentSize
                            + " newSize=" + newSize
                            + " usedMemorySize=" + usedMemorySize
                            + " delta=" + (pointer - address)
            );
        }

        if (newSize <= currentSize) {
            throw new IllegalStateException("Can't acquire size which is less or equal to the current size");
        }

        long size = newSize - currentSize;
        usedMemorySize += size;

        if (requireNewMemoryBlock(address, newSize)) {
            allocateBlock(newSize);
            MEMORY_ACCESSOR.copyMemory(address, pointer, currentSize);
            long oldPointer = pointer;
            pointer += newSize;
            return oldPointer;
        } else {
            pointer += size;
            return address;
        }
    }

    private boolean requireNewMemoryBlock(long address, long size) {
        return (currentMemoryBlock == null) || (currentMemoryBlock.address + currentMemoryBlock.size < address + size);
    }

    @Override
    public void free(long address, long size) {
        throw new UnsupportedOperationException("Free is unsupported for append only storage");
    }

    @Override
    public MemoryAllocator unwrapMemoryAllocator() {
        return memoryAllocator;
    }

    @Override
    public void compact() {
        throw new UnsupportedOperationException("Compact is unsupported for append only storage");
    }

    @Override
    public void destroy() {
        if (!isDestroyed) {
            try {
                for (MemoryBlock memoryBlock : memoryBlockList) {
                    memoryAllocator.free(
                            memoryBlock.address,
                            memoryBlock.size
                    );
                }
            } finally {
                reset();
                memoryBlockList.clear();
                acquiredMemorySize = 0L;
                isDestroyed = true;
            }
        }
    }

    @Override
    public boolean isDestroyed() {
        return isDestroyed;
    }

    @Override
    public JVMMemoryStats getMemoryStats() {
        return memoryStats;
    }

    @Override
    public long getUsableSize(long address) {
        return SIZE_INVALID;
    }

    @Override
    public long validateAndGetUsableSize(long address) {
        return SIZE_INVALID;
    }

    @Override
    public long getAllocatedSize(long address) {
        return SIZE_INVALID;
    }

    @Override
    public long validateAndGetAllocatedSize(long address) {
        return SIZE_INVALID;
    }

    @Override
    public long newSequence() {
        return sequenceGenerator.inc();
    }

    @Override
    public void reset() {
        pointer = 0L;
        usedMemorySize = 0L;
        currentBlockIndex = 0;
        acquiredMemorySize = 0L;
        currentMemoryBlock = null;
    }

    private static class MemoryBlock {

        private final long size;
        private final long address;

        MemoryBlock(long address, long size) {
            this.address = address;
            this.size = size;
        }
    }
}
