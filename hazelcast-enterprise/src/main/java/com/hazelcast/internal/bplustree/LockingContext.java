package com.hazelcast.internal.bplustree;

import com.hazelcast.internal.util.collection.HsaHeapMemoryManager;
import com.hazelcast.internal.util.collection.LongCursor;
import com.hazelcast.internal.util.collection.LongSet;
import com.hazelcast.internal.util.collection.LongSetHsa;

import static com.hazelcast.internal.memory.impl.LibMalloc.NULL_ADDRESS;

/**
 * Keeps track of the acquired locks on the B+tree nodes. Can be used to release all locks when exceptions like
 * {@link com.hazelcast.memory.NativeOutOfMemoryError} or {@link BPlusTreeLimitException} are thrown.
 */
class LockingContext {

    private final LongSet set;

    LockingContext() {
        set = new LongSetHsa(NULL_ADDRESS, new HsaHeapMemoryManager());
    }

    void addLock(long lockAddr) {
        set.add(lockAddr);
    }

    void removeLock(long lockAddr) {
        set.remove(lockAddr);
    }

    void releaseLocks(LockManager lockManager) {
        LongCursor cursor = set.cursor();
        while (cursor.advance()) {
            long lockAddr = cursor.value();
            lockManager.releaseLock(lockAddr);
        }
        set.clear();
    }

    boolean hasNoLocks() {
        return set.isEmpty();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("LockingContext{ ");
        LongCursor cursor = set.cursor();
        while (cursor.advance()) {
            long lockAddr = cursor.value();
            builder.append(lockAddr);
            builder.append(" ");
        }
        builder.append("}");
        return builder.toString();
    }

}
