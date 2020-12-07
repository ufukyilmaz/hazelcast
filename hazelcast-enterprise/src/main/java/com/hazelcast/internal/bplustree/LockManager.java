package com.hazelcast.internal.bplustree;

/**
 * Represents an API of lock manager built for synchronization of off-heap data structures.
 */
interface LockManager {

    /**
     * Acquires the read lock.
     * <p>
     * Acquires the read lock if the write lock is not held by another caller and returns immediately.
     * <p>
     * If the write lock is held by another caller then the current thread becomes disabled
     * for thread scheduling purposes and lies dormant until the read lock has been acquired.
     *
     * @param lockAddr the address of the lock state associated with the resource to be locked
     */
    void readLock(long lockAddr);

    /**
     * Acquires the read lock only if the write lock is not held by another caller.
     * <p>
     * If write lock is held by another caller then this method will return immediately with the
     * value {@code false}.
     *
     * @param lockAddr the address of the lock state associated with the resource to be locked
     * @return {@code true} if the lock was free and was acquired, {@code false} otherwise.
     */
    boolean tryReadLock(long lockAddr);

    /**
     * Instantly acquires the read lock.
     * <p>
     * Acquires (and immediately releases) the read lock if the write lock is not held by another caller
     * and returns immediately.
     * <p>
     * The method is useful to prevent busy waits.
     *
     * @param lockAddr the address of the lock state associated with the resource to be locked
     */
    void instantDurationReadLock(long lockAddr);

    /**
     * Acquires the write lock.
     * Acquires the write lock if neither the read nor write lock are held by another caller and returns immediately.
     * <p>
     * If the lock is held by another caller then the current thread becomes disabled for thread scheduling purposes
     * and lies dormant until the write lock has been acquired.
     *
     * @param lockAddr the address of the lock state associated with the resource to be locked
     */
    void writeLock(long lockAddr);

    /**
     * Upgrades from a read lock to the write lock if neither the read nor write lock are held by another caller
     * and returns immediately.
     * <p>
     * If either read or write lock is held by another caller then this method will return immediately with the
     * value {@code false}.
     * <p>
     * It is up to the caller to correctly use this method, so that the caller must already held the read lock.
     * If the read lock is held by another caller, the upgrade will succeed and may cause a deadlock or corruption
     * of the underlying data structure.
     *
     * @param lockAddr the address of the lock state associated with the resource to be locked
     * @return {@code true} if the lock was upgraded, {@code false} otherwise.
     */
    boolean tryUpgradeToWriteLock(long lockAddr);

    /**
     * Acquires the write lock only if neither the read nor write lock are held by another caller and returns immediately.
     * <p>
     * If either read or write lock is held by another caller then this method will return immediately with the
     * value {@code false}.
     *
     * @param lockAddr the address of the lock state associated with the resource to be locked
     * @return {@code true} if the lock was free and was acquired, {@code false} otherwise.
     */
    boolean tryWriteLock(long lockAddr);

    /**
     * Instantly acquires the write lock.
     * <p>
     * Acquires (and immediately releases) the write lock if neither the read nor write lock are held by another caller
     * and returns immediately.
     * <p>
     * The method is useful to prevent busy waits.
     *
     * @param lockAddr the address of the lock state associated with the resource to be locked
     */
    void instantDurationWriteLock(long lockAddr);

    /**
     * Releases the lock acquired previously by the caller.
     * <p>
     * It is up to the caller to pair correctly acquisition and release of the lock. Inappropriate call
     * of the releaseLock may release the lock acquired by another logical operation, what in turn may cause
     * a corruption of the underlying data structure.
     *
     * @param lockAddr the address of the lock state associated with the resource to be locked
     */
    void releaseLock(long lockAddr);

}
