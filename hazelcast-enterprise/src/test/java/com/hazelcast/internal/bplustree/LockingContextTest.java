package com.hazelcast.internal.bplustree;

import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.commons.collections.CollectionUtils;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;

import static com.hazelcast.internal.bplustree.BPlusTreeLockingTest.DelegatingLockManager;
import static com.hazelcast.internal.bplustree.BPlusTreeLockingTest.LockManagerCallback;
import static com.hazelcast.internal.bplustree.HDLockManagerTest.assertLockState;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LockingContextTest extends BPlusTreeTestSupport {

    private ArrayList<Long> releasedLocks = new ArrayList<>();
    private boolean throwOOME;

    @After
    public void tearDown() {
        // no-op
    }

    @Override
    DelegatingMemoryAllocator newDelegatingIndexMemoryAllocator(MemoryAllocator indexAllocator, AllocatorCallback callback) {
        return new DelegatingMemoryAllocator(new DelegatingIndexAllocator(indexAllocator), callback);
    }

    @Test
    public void testLockingContextOps() {
        LockingContext lockingContext = new LockingContext();

        assertTrue(lockingContext.hasNoLocks());
        addLocks(lockingContext);

        assertFalse(lockingContext.hasNoLocks());
        lockingContext.removeLock(1L);
        assertFalse(lockingContext.hasNoLocks());
        lockingContext.removeLock(2L);
        assertFalse(lockingContext.hasNoLocks());
        lockingContext.removeLock(3L);
        assertTrue(lockingContext.hasNoLocks());
    }

    @Test
    public void testReleaseLocks() {
        LockManager lockManager = new HDLockManager(32);
        DelegatingLockManager delegating = new DelegatingLockManager(lockManager, new LockManagerCallbackOnRelease());
        long lock1 = allocateZeroed(8);
        long lock2 = allocateZeroed(8);
        long lock3 = allocateZeroed(8);

        LockingContext lockingContext = new LockingContext();

        delegating.readLock(lock1);
        lockingContext.addLock(lock1);
        delegating.writeLock(lock2);
        lockingContext.addLock(lock2);
        delegating.writeLock(lock3);
        lockingContext.addLock(lock3);

        lockingContext.releaseLocks(delegating);

        assertTrue(lockingContext.hasNoLocks());
        assertTrue(CollectionUtils.isEqualCollection(releasedLocks, Arrays.asList(lock1, lock2, lock3)));
        keyAllocator.free(lock1, 8);
        keyAllocator.free(lock2, 8);
        keyAllocator.free(lock3, 8);
    }

    @Test
    public void testOOME_OnInsert() {
        // Fill in the leaf node
        insertKeys(9);
        long leaf = innerNodeAccessor.getValueAddr(rootAddr, 0);

        // Next allocate (leaf split) will throw an exception
        throwOOME = true;

        try {
            insertKey(10);
            fail("Should throw NativeOutOfMemoryError");
        } catch (NativeOutOfMemoryError oom) {
            assertLockState(rootAddr, 0, 0, 0);
            assertLockState(leaf, 0, 0, 0);
        }
    }

    private void addLocks(LockingContext lockingContext) {
        lockingContext.addLock(1L);
        lockingContext.addLock(2L);
        lockingContext.addLock(3L);
    }

    private class LockManagerCallbackOnRelease implements LockManagerCallback {

        @Override
        public void onReadLock(long lockAddr) {
            // no-op
        }

        @Override
        public void onTryReadLock(long lockAddr, boolean result) {
            // no-op
        }

        @Override
        public void onInstantReadLock(long lockAddr) {
            // no-op
        }

        @Override
        public void onWriteLock(long lockAddr) {
            // no-op
        }

        @Override
        public void onTryUpgradeLock(long lockAddr, boolean result) {
            // no-op
        }

        @Override
        public void onTryWriteLock(long lockAddr, boolean result) {
            // no-op
        }

        @Override
        public void onInstantWriteLock(long lockAddr) {
            // no-op
        }

        @Override
        public void onReleaseLock(long lockAddr) {
            releasedLocks.add(lockAddr);
        }
    }

    private class DelegatingIndexAllocator implements MemoryAllocator {

        private final MemoryAllocator delegate;

        DelegatingIndexAllocator(MemoryAllocator delegate) {
            this.delegate = delegate;
        }

        void checkThrowOOME() {
            if (throwOOME) {
                throw new NativeOutOfMemoryError();
            }
        }

        @Override
        public long allocate(long size) {
            checkThrowOOME();
            return delegate.allocate(size);
        }

        @Override
        public long reallocate(long address, long currentSize, long newSize) {
            checkThrowOOME();
            return delegate.reallocate(address, currentSize, newSize);
        }

        @Override
        public void free(long address, long size) {
            delegate.free(address, size);
        }

        @Override
        public void dispose() {
            delegate.dispose();
        }
    }
}
