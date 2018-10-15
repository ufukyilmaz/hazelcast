package com.hazelcast.memory;

/**
 * Reasoning:
 * Destruction of memory manager also destroys its internal address queues.
 * But later, it is possible that a thread can access these queues and can
 * get NPE. By using this class, instead of NPE, we want to return a
 * meaningful exception message to the user which indicates the reason of
 * failure.
 */
final class DestroyedAddressQueue implements AddressQueue {

    static final DestroyedAddressQueue INSTANCE = new DestroyedAddressQueue();

    private DestroyedAddressQueue() {
    }

    private static RuntimeException newIllegalStateException() {
        return new IllegalStateException("Memory manager was already destroyed!");
    }

    @Override
    public int getIndex() {
        throw newIllegalStateException();
    }

    @Override
    public long acquire() {
        throw newIllegalStateException();
    }

    @Override
    public boolean release(long memory) {
        throw newIllegalStateException();
    }

    @Override
    public int getMemorySize() {
        throw newIllegalStateException();
    }

    @Override
    public int capacity() {
        throw newIllegalStateException();
    }

    @Override
    public int remaining() {
        throw newIllegalStateException();
    }

    @Override
    public boolean beforeCompaction() {
        throw newIllegalStateException();
    }

    @Override
    public void afterCompaction() {
        throw newIllegalStateException();
    }

    @Override
    public void destroy() {
        throw newIllegalStateException();
    }
}
