package com.hazelcast.spi.hotrestart.impl.gc.mem;

import com.hazelcast.internal.nio.Disposable;
import com.hazelcast.spi.hotrestart.HotRestartException;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;

import static com.hazelcast.internal.nio.IOUtil.delete;
import static com.hazelcast.internal.util.QuickMath.log2;
import static com.hazelcast.internal.util.QuickMath.nextPowerOfTwo;

/**
 * Represents a single memory-mapped file used as a slab of equal-sized memory blocks.
 * Although there is only one file, it's mapped using several memory-mapped buffers.
 * When the file needs expanding to accommodate more blocks, the existing buffer cannot be disposed
 * because the addresses inside it are allocated and pinned. Therefore on each expansion an additional
 * memory-mapped buffer is created.
 * <p>
 * Implemented in terms of reflective calls to the private OpenJDK methods
 * {@link sun.nio.ch.FileChannelImpl#map0(int, long, long)} and
 * {@link sun.nio.ch.FileChannelImpl#unmap0(long, long)}.
 */
final class MmapSlab implements Disposable {

    private static final int MAPMODE_RW = 1;

    private final int mmapPageSize;
    private final long blockSize;
    private final int initialBlockCountLog2;
    private final BitSet blockBitmap = new BitSet();
    private final RandomAccessFile raf;
    private final List<Long> bufBases = new ArrayList<Long>();
    private final List<Integer> offsetsOfBufsFromMmmapBases = new ArrayList<Integer>();
    private final NavigableMap<Long, Integer> bufBase2BufIndex = new TreeMap<Long, Integer>();
    private int blockCount;
    private int usedBlockCount;
    private final File mappedFile;

    MmapSlab(File baseDir, long blockSize) {
        this.blockSize = blockSize;
        try {
            this.mappedFile = new File(baseDir, blockSize + ".mmap");
            this.raf = new RandomAccessFile(mappedFile, "rw");
        } catch (Exception e) {
            throw new HotRestartException("Failed to create a MmapSlab for blockSize " + blockSize, e);
        }
        try {
            this.mmapPageSize = (int) mmapAllocationGranularity(raf.getChannel());
            final int minFileSize = 2 * mmapPageSize;
            final int initialBlockCount = (int) nextPowerOfTwo(minFileSize / blockSize);
            this.initialBlockCountLog2 = log2(initialBlockCount);
            mmapExpand(initialBlockCount);
        } catch (HotRestartException e) {
            delete(mappedFile);
            throw e;
        }
    }

    long allocate() {
        assert !bufBases.isEmpty() : "MmapSlab disposed";
        // All blocks, across all mapped byte buffers, are globally ordered and this is the index
        // of the first free block
        final int freeBlockIndex = blockBitmap.nextClearBit(0);
        if (freeBlockIndex == blockCount) {
            mmapExpand(blockCount);
        }
        assert freeBlockIndex < blockCount
                : String.format("freeBlockIndex %d >= blockCount %d even after expanding", freeBlockIndex, blockCount);
        final long blockBase = indexToBlockBase(freeBlockIndex);
        blockBitmap.set(freeBlockIndex);
        usedBlockCount++;
        return blockBase;
    }

    boolean free(long blockBase) {
        assert !bufBases.isEmpty() : "MmapSlab disposed";
        blockBitmap.clear(blockBaseToIndex(blockBase));
        return --usedBlockCount == 0;
    }

    /**
     * @param blockIndex index of a block in the underlying file
     * @return the block's base address in RAM
     */
    long indexToBlockBase(int blockIndex) {
        final int blockIndexLog2 = log2(blockIndex);
        // Index of the mapped byte buffer (inside the list of all mapped byte buffers) which contains the block
        final int indexOfBuf = Math.max(0, blockIndexLog2 - initialBlockCountLog2 + 1);
        // Base address of the mapped byte buffer.
        final long bufBase = bufBases.get(indexOfBuf);
        // Global index of the first block in the mapped byte buffer
        final int indexAtBufBase = (indexOfBuf == 0 ? 0 : (1 << blockIndexLog2));
        // Index of the block inside the mapped byte buffer
        final int bufRelativeIndex = blockIndex - indexAtBufBase;
        // Offset of block base relative to the buffer base
        final long blockOffset = blockSize * bufRelativeIndex;
        // Base address of the selected free block
        return bufBase + blockOffset;
    }

    /**
     * @param blockBase base RAM address of a block
     * @return index of the block in the underlying file
     */
    int blockBaseToIndex(long blockBase) {
        final Entry<Long, Integer> floorEntry = bufBase2BufIndex.floorEntry(blockBase);
        final long bufBase = floorEntry.getKey();
        final long blockOffset = blockBase - bufBase;
        assert blockOffset % blockSize == 0 : String.format("Block with base address %,d doesn't belong to this slab."
                + " Resolved buffer base %,d, block offset %,d. Block size %,d, blockOffset %% blockSize %,d",
                blockBase, bufBase, blockOffset, blockSize, blockOffset % blockSize);
        final long bufRelativeBlockIndex = blockOffset / blockSize;
        final int indexOfBuf = floorEntry.getValue();
        final int indexAtBufBase = (indexOfBuf == 0 ? 0 : 1 << (indexOfBuf - 1 + initialBlockCountLog2));
        final long blockIndex = indexAtBufBase + bufRelativeBlockIndex;
        assert blockIndex < blockCount
                : String.format("Block with base address %,d doesn't belong to this slab."
                + " Resolved buffer base %,d, block offset %,d, index at buffer base %,d,"
                + " buffer-relative block index %,d, global block index %,d. Total block count %,d",
                blockBase, bufBase, blockOffset, indexAtBufBase, bufRelativeBlockIndex, blockIndex, blockCount);
        return (int) blockIndex;
    }

    private void mmapExpand(long addedBlockCount) {
        final long addedFileSize = addedBlockCount * blockSize;
        final long newFileSize = (blockCount + addedBlockCount) * blockSize;
        try {
            raf.setLength(newFileSize);
            final FileChannel chan = raf.getChannel();
            final Method map0 = chan.getClass().getDeclaredMethod("map0", int.class, long.class, long.class);
            map0.setAccessible(true);
            final long fileposOfNewBuffer = blockCount * blockSize;
            final long offsetOfBufIntoMmapPage = fileposOfNewBuffer % mmapPageSize;
            final long fileposOfMappedRegion = fileposOfNewBuffer - offsetOfBufIntoMmapPage;
            final long sizeOfMappedRegion = addedFileSize + offsetOfBufIntoMmapPage;
            final long mmapBase = (Long) map0.invoke(chan, MAPMODE_RW, fileposOfMappedRegion, sizeOfMappedRegion);
            final long bufBase = mmapBase + offsetOfBufIntoMmapPage;
            bufBases.add(bufBase);
            offsetsOfBufsFromMmmapBases.add((int) offsetOfBufIntoMmapPage);
            bufBase2BufIndex.put(bufBase, bufBases.size() - 1);
            blockCount += addedBlockCount;
        } catch (Exception e) {
            throw new HotRestartException(String.format("mmap allocation failed."
                    + " addedFileSize %,d newFileSize %,d blockCount %,d blockSize %,d",
                    addedFileSize, newFileSize, blockCount, blockSize), e);
        }
    }

    @Override
    public void dispose() {
        if (bufBases.isEmpty()) {
            return;
        }
        try {
            final FileChannel chan = raf.getChannel();
            final Method unmap0 = chan.getClass().getDeclaredMethod("unmap0", long.class, long.class);
            unmap0.setAccessible(true);
            long blockCount = 1L << initialBlockCountLog2;
            boolean atFirstBuffer = true;
            for (int i = 0; i < bufBases.size(); i++) {
                final long bufBase = bufBases.get(i);
                final int bufOffsetFromMmapBase = offsetsOfBufsFromMmmapBases.get(i);
                unmap0.invoke(chan, bufBase - bufOffsetFromMmapBase, blockCount * blockSize + bufOffsetFromMmapBase);
                if (atFirstBuffer) {
                    atFirstBuffer = false;
                } else {
                    blockCount *= 2;
                }
            }
            bufBases.clear();
            raf.close();
            delete(mappedFile);
        } catch (NoSuchMethodException e) {
            throw new HotRestartException("unmap0 method not found", e);
        } catch (InvocationTargetException e) {
            throw new HotRestartException("Reflection error accessing unmap0", e);
        } catch (IllegalAccessException e) {
            throw new HotRestartException("Reflection error accessing unmap0", e);
        } catch (IOException e) {
            throw new HotRestartException("Failed to close RAF", e);
        }
    }

    private static long mmapAllocationGranularity(FileChannel channel) {
        try {
            final Field granularity = channel.getClass().getDeclaredField("allocationGranularity");
            granularity.setAccessible(true);
            return (Long) granularity.get(null);
        } catch (Exception e) {
            throw new HotRestartException("Failed to retrieve mmap allocation granularity", e);
        }
    }
}
