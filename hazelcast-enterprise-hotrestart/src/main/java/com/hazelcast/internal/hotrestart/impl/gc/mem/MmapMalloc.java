package com.hazelcast.internal.hotrestart.impl.gc.mem;

import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.nio.Disposable;
import com.hazelcast.hotrestart.HotRestartException;
import com.hazelcast.internal.util.collection.Long2ObjectHashMap;

import java.io.File;

/**
 * Allocator based on memory-mapped files. Uses a set of {@link MmapSlabs}, one for each
 * block size.
 */
public class MmapMalloc implements MemoryAllocator, Disposable {
    private final File baseDir;
    private final Long2ObjectHashMap<MmapSlab> slabs = new Long2ObjectHashMap<MmapSlab>();
    private final boolean deleteEmptySlab;

    public MmapMalloc(File baseDir, boolean deleteEmptySlab) {
        this.deleteEmptySlab = deleteEmptySlab;
        if (!baseDir.exists() && !baseDir.mkdirs()) {
            throw new HotRestartException("Could not create the mmap base directory " + baseDir);
        }
        this.baseDir = baseDir;
    }

    @Override
    public long allocate(long size) {
        MmapSlab slab = slabs.get(size);
        if (slab == null) {
            slab = new MmapSlab(baseDir, size);
            slabs.put(size, slab);
        }
        return slab.allocate();
    }

    @Override
    public long reallocate(long address, long currentSize, long newSize) {
        throw new UnsupportedOperationException("reallocate");
    }

    @Override
    public void free(long address, long size) {
        final MmapSlab slab = slabs.get(size);
        if (slab.free(address) && deleteEmptySlab) {
            slab.dispose();
            slabs.remove(size);
        }
    }

    @Override
    public void dispose() {
        for (MmapSlab slab : slabs.values()) {
            slab.dispose();
        }
    }
}
