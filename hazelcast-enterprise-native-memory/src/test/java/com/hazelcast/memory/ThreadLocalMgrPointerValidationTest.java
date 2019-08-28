package com.hazelcast.memory;

import com.hazelcast.internal.memory.impl.LibMalloc;
import com.hazelcast.nio.Bits;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;
import static com.hazelcast.memory.HazelcastMemoryManager.SIZE_INVALID;
import static com.hazelcast.memory.ThreadLocalPoolingMemoryManager.AVAILABLE_BIT;
import static com.hazelcast.memory.ThreadLocalPoolingMemoryManager.BLOCK_SIZE_MASK;
import static com.hazelcast.memory.ThreadLocalPoolingMemoryManager.HEADER_SIZE;
import static com.hazelcast.memory.ThreadLocalPoolingMemoryManager.MEMORY_OVERHEAD_WHEN_PAGE_OFFSET_IS_STORED;
import static com.hazelcast.memory.ThreadLocalPoolingMemoryManager.createAvailableHeader;
import static com.hazelcast.memory.ThreadLocalPoolingMemoryManager.encodeSize;
import static com.hazelcast.memory.ThreadLocalPoolingMemoryManager.makeHeaderUnavailable;
import static com.hazelcast.memory.ThreadLocalPoolingMemoryManager.markAsExternalBlockHeader;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ThreadLocalMgrPointerValidationTest extends ParameterizedMemoryTest {

    private static final int BLOCK_OVERHEAD_WITH_OFFSET_STORED = MEMORY_OVERHEAD_WHEN_PAGE_OFFSET_IS_STORED;
    private static final int BLOCK_OVERHEAD_WITHOUT_OFFSET_STORED = HEADER_SIZE;

    private final int pageSize = 1 << 9;
    private final int legitBlockSize = 1 << 7;
    private final int bogusBlockSize = legitBlockSize / 2;

    private ThreadLocalPoolingMemoryManager mgr;
    private LibMalloc libMalloc;

    private long legitAddr;
    private long bogusAddr;
    private byte bogusHeader;
    // same for both legit and bogus blocks because it is at the end of the block
    private long addressOfStoredPageOffset;
    private int bogusPageOffset;

    @Before
    public void before() {
        int minBlockSize = 1 << 4;
        long maxNative = 1 << 20;
        long maxMetadata = 1 << 20;

        libMalloc = newLibMalloc(persistentMemory);
        mgr = new ThreadLocalPoolingMemoryManager(minBlockSize, pageSize, libMalloc,
                new PooledNativeMemoryStats(maxNative, maxMetadata));
    }

    @After
    public void after() {
        if (mgr != null) {
            mgr.dispose();
        }
        if (libMalloc != null) {
            libMalloc.dispose();
        }
    }

    private void setUpForTestWithStoredOffset() {
        legitAddr = mgr.allocate(legitBlockSize - BLOCK_OVERHEAD_WITH_OFFSET_STORED);
        bogusAddr = toBogusAddr(legitAddr);
        bogusHeader = forgeBogusHeader(true);
        addressOfStoredPageOffset = addressOfStoredPageOffset(legitAddr);
        bogusPageOffset = forgeBogusPageOffset(legitAddr, AMEM.getInt(addressOfStoredPageOffset), bogusAddr);
    }

    private void setUpForTestWithoutStoredOffset() {
        legitAddr = mgr.allocate(legitBlockSize - BLOCK_OVERHEAD_WITHOUT_OFFSET_STORED);
        bogusAddr = toBogusAddr(legitAddr);
        bogusHeader = forgeBogusHeader(false);
    }

    @Test
    public void perfectCorruptionWithStoredOffset_goesUndetected() {
        // Given
        setUpForTestWithStoredOffset();

        // When
        storeBogusHeader();
        storeBogusPageOffset();

        // Then
        assertEquals(bogusBlockSize, mgr.validateAndGetAllocatedSize(bogusAddr));
    }

    @Test
    public void perfectCorruptionWithoutStoredOffset_goesUndetected() {
        // Given
        setUpForTestWithoutStoredOffset();

        // When
        storeBogusHeader();

        // Then
        assertEquals(bogusBlockSize, mgr.validateAndGetAllocatedSize(bogusAddr));
    }

    @Test
    public void corruptionWithExternalBitSet_detected() {
        // Given
        setUpForTestWithStoredOffset();

        // When
        bogusHeader = markAsExternalBlockHeader(bogusHeader);
        storeBogusHeader();
        storeBogusPageOffset();

        // Then
        assertEquals(SIZE_INVALID, mgr.validateAndGetAllocatedSize(bogusAddr));
    }

    @Test
    public void corruptionWithAvailableHeader_detected() {
        // Given
        setUpForTestWithStoredOffset();

        // When
        bogusHeader = Bits.setBit(bogusHeader, AVAILABLE_BIT);
        storeBogusHeader();
        storeBogusPageOffset();

        // Then
        assertEquals(SIZE_INVALID, mgr.validateAndGetAllocatedSize(bogusAddr));
    }

    @Test
    public void corruptionWithTooSmallDecodedSize_detected() {
        // Given
        setUpForTestWithStoredOffset();

        // When
        setBogusBlockSize(mgr.minBlockSize / 2);
        storeBogusHeader();

        // Then
        assertEquals(SIZE_INVALID, mgr.validateAndGetAllocatedSize(bogusAddr));
    }

    @Test
    public void corruptionWithTooLargeDecodedSize_detected() {
        // Given
        setUpForTestWithStoredOffset();

        // When
        setBogusBlockSize(mgr.pageSize * 2);
        storeBogusHeader();

        // Then
        assertEquals(SIZE_INVALID, mgr.validateAndGetAllocatedSize(bogusAddr));
    }

    @Test
    public void corruptionWithLegalDecodedSize_butExtendingBeyondPageEnd_detectedWithoutCrash() {
        // Given
        setUpForTestWithStoredOffset();

        // When
        setBogusBlockSize(mgr.pageSize);
        storeBogusHeader();

        // Then
        assertEquals(SIZE_INVALID, mgr.validateAndGetAllocatedSize(bogusAddr));
    }

    @Test
    public void corruptionWithNegativePageOffset_detected() {
        // Given
        setUpForTestWithStoredOffset();

        // When
        storeBogusHeader();
        bogusPageOffset = -1;
        storeBogusPageOffset();

        // Then
        assertEquals(SIZE_INVALID, mgr.validateAndGetAllocatedSize(bogusAddr));
    }

    @Test
    public void corruptionWithNonMatchingPageOffset_detected() {
        // Given
        setUpForTestWithStoredOffset();

        // When
        storeBogusHeader();
        bogusPageOffset += 8;
        storeBogusPageOffset();

        // Then
        assertEquals(SIZE_INVALID, mgr.validateAndGetAllocatedSize(bogusAddr));
    }

    @Test
    public void deallocatedExtBlock_detected() {
        // Given
        final int size = 2 * pageSize;
        final long addr = mgr.allocate(size);
        mgr.free(addr, size);

        // When - Then
        assertEquals(SIZE_INVALID, mgr.validateAndGetAllocatedSize(addr));
    }


    private long toBogusAddr(long legitAddr) {
        // bogus block is at the end of the legit block
        return legitAddr + (legitBlockSize - bogusBlockSize);
    }

    private byte forgeBogusHeader(boolean canStorePageOffset) {
        return makeHeaderUnavailable(createAvailableHeader(bogusBlockSize), canStorePageOffset);
    }

    private int forgeBogusPageOffset(long legitAddr, int legitPageOffset, long bogusAddr) {
        return legitPageOffset + (int) (bogusAddr - legitAddr);
    }

    private void setBogusBlockSize(int size) {
        bogusHeader &= ~BLOCK_SIZE_MASK; // zero out the size field
        bogusHeader |= encodeSize(size);
    }

    private long addressOfStoredPageOffset(long legitAddr) {
        return ThreadLocalPoolingMemoryManager.addressOfStoredPageOffset(legitAddr, legitBlockSize);
    }

    private void storeBogusHeader() {
        AMEM.putByte(mgr.toHeaderAddress(bogusAddr), bogusHeader);
    }

    private void storeBogusPageOffset() {
        AMEM.putInt(addressOfStoredPageOffset, bogusPageOffset);
    }
}
