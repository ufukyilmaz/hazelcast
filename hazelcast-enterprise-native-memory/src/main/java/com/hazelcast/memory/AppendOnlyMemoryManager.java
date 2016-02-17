package com.hazelcast.memory;

import java.util.List;
import java.util.ArrayList;

import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.internal.memory.MemoryAccessorProvider;
import com.hazelcast.internal.memory.MemoryAccessorType;
import com.hazelcast.internal.memory.impl.UnsafeMalloc;
import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.internal.util.counters.MwCounter;

public class AppendOnlyMemoryManager implements MemoryManager, Resetable {

    // We are using `STANDARD` memory accessor because we internally guarantee that
    // every memory access is aligned.
    private static final MemoryAccessor MEMORY_ACCESSOR =
            MemoryAccessorProvider.getMemoryAccessor(MemoryAccessorType.STANDARD);

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

    public AppendOnlyMemoryManager(MemorySize cap,
                                   int minBlockSize) {
        this.minBlockSize = minBlockSize;
        this.memoryStats = new NativeMemoryStats(cap.bytes());
        this.memoryAllocator = new StandardMemoryManager(
                new UnsafeMalloc(),
                this.memoryStats
        );
    }

    private void allocateBlock(long desiredBlockSize) {
        long blockSize = Math.max(desiredBlockSize, this.minBlockSize);
        this.acquiredMemorySize += blockSize;

        if (this.currentBlockIndex < this.memoryBlockList.size() - 1) {
            this.currentBlockIndex++;
            MemoryBlock block = this.memoryBlockList.get(this.currentBlockIndex);

            if (blockSize > block.size) {
                while ((this.memoryBlockList.size() - 1 >= this.currentBlockIndex)) {
                    block = this.memoryBlockList.get(this.currentBlockIndex);

                    if (blockSize > block.size) {
                        this.memoryAllocator.free(block.address, block.size);
                        this.memoryBlockList.remove(this.currentBlockIndex);
                    } else {
                        this.currentMemoryBlock = block;
                        this.pointer = block.address;
                        return;
                    }
                }
            } else {
                this.currentMemoryBlock = block;
                this.pointer = block.address;
                return;
            }
        }

        this.pointer = this.memoryAllocator.allocate(blockSize);

        this.currentMemoryBlock = new MemoryBlock(
                this.pointer,
                blockSize
        );
        this.memoryBlockList.add(this.currentMemoryBlock);
        this.currentBlockIndex = this.memoryBlockList.size() - 1;
    }

    @Override
    public long allocate(long size) {
        this.usedMemorySize += size;

        if (requireNewMemoryBlock(this.pointer, size)) {
            allocateBlock(this.usedMemorySize - this.acquiredMemorySize);
        }

        long pointer = this.pointer;
        this.pointer += size;
        return pointer;
    }

    @Override
    public long reallocate(long address, long currentSize, long newSize) {
        if (this.pointer - currentSize != address) {
            throw new IllegalStateException(
                    "Can't re-allocate not last allocated memory currentSize=" + currentSize +
                            " newSize=" + newSize +
                            " usedMemorySize=" + this.usedMemorySize +
                            " delta=" + (this.pointer - address)
            );
        }

        if (newSize <= currentSize) {
            throw new IllegalStateException("Can't acquire size which is less or equal to the current size");
        }

        long size = newSize - currentSize;
        this.usedMemorySize += size;

        if (requireNewMemoryBlock(address, newSize)) {
            allocateBlock(newSize);
            MEMORY_ACCESSOR.copyMemory(address, this.pointer, currentSize);
            long pointer = this.pointer;
            this.pointer += newSize;
            return pointer;
        } else {
            this.pointer += size;
            return address;
        }
    }

    private boolean requireNewMemoryBlock(long address, long size) {
        return (this.currentMemoryBlock == null) || (this.currentMemoryBlock.address + this.currentMemoryBlock.size < address + size);
    }

    @Override
    public void free(long address, long size) {
        throw new UnsupportedOperationException("Free is unsupported for append only storage");
    }

    @Override
    public MemoryAllocator unwrapMemoryAllocator() {
        return this.memoryAllocator;
    }

    @Override
    public void compact() {
        throw new UnsupportedOperationException("Compact is unsupported for append only storage");
    }

    @Override
    public void destroy() {
        if (!this.isDestroyed) {
            try {
                for (MemoryBlock memoryBlock : this.memoryBlockList) {
                    this.memoryAllocator.free(
                            memoryBlock.address,
                            memoryBlock.size
                    );
                }
            } finally {
                reset();
                this.memoryBlockList.clear();
                this.acquiredMemorySize = 0L;
                this.isDestroyed = true;
            }
        }
    }

    @Override
    public boolean isDestroyed() {
        return this.isDestroyed;
    }

    @Override
    public MemoryStats getMemoryStats() {
        return this.memoryStats;
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
        return this.sequenceGenerator.inc();
    }

    @Override
    public void reset() {
        this.pointer = 0L;
        this.usedMemorySize = 0L;
        this.currentBlockIndex = 0;
        this.acquiredMemorySize = 0L;
        this.currentMemoryBlock = null;
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
