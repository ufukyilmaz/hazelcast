package com.hazelcast.internal.bplustree;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;
import static com.hazelcast.internal.util.HashUtil.fastLongMix;
import static com.hazelcast.internal.util.QuickMath.isPowerOfTwo;

/**
 * Provides implementation of off-heap Lock Manager (LM).
 * <p>
 * To perform locking operations, the caller provides an off-heap address of
 * the memory (that is a lock address), which is used by LM to store the lock's state.
 * <p>
 * The lock state is represented with 8 bytes (long).
 * <p>
 * The low 2 bytes are used for the users count of the lock.
 * The 0 users count indicates that there is no lock, -1 value means exclusive lock is acquired
 * and the value grater than zero indicated the number of acquired shared locks.
 * <p>
 * The next 2 bytes are used for the read waiters count, followed by the 2 bytes used for the write waiters count.
 * The high 2 bytes are not used and reserved for future needs.
 * <p>
 * The LM uses on-heap ReentrantLock to support internally wait/notify functionality.
 * The LM uses striped approach to handle unlimited number of off-heap locks with a limited number
 * of ReentrantLocks.
 * <p>
 * The striped approach limits the number of on-heap objects instantiated by the Lock Manager,
 * but may cause an unnecessary wakeup of the blocked caller. The stripes count parameter is a trade-off
 * between a number of synchronization objects and unnecessary wakeup(s).
 * <p>
 * The readLock and writeLock methods employ a 2 stages approach. On the first stage the spinning approach is
 * used. It tries to acquire a lock using relatively cheap CAS operation and if it fails after {@link #SPIN_COUNT}
 * attempts it proceeds with the second stage. On the second stage more expensive ReentrantLock is used.
 * <p>
 * In general, the LM favors writers over readers if both are awaiting for a lock.
 * <p>
 * The LM doesn't have internal deadlock detector and it is up to the user to prevent deadlock scenarios.
 * <p>
 * The read and write locks are not reentrant.
 */
@SuppressWarnings("checkstyle:MethodCount")
public final class HDLockManager implements LockManager {

    // The maximum number of shared users of the lock
    private static final int MAX_USERS_COUNT = Short.MAX_VALUE;

    // The maximum number of read/write waiters
    private static final int MAX_WAITERS_COUNT = 0xFFFF;

    // The mask is used to clear the users count in the lock state
    @SuppressWarnings("checkstyle:magicnumber")
    private static final long CLEARING_USERS_COUNT_MASK = 0xFFFFFFFFFFFF0000L;

    // The mask is used to clear the read waiters count in the lock state
    @SuppressWarnings("checkstyle:magicnumber")
    private static final long CLEARING_READ_WAITERS_COUNT_MASK = 0xFFFFFFFF0000FFFFL;

    // The mask is used to clear the write waiters count in the lock state
    @SuppressWarnings("checkstyle:magicnumber")
    private static final long CLEARING_WRITE_WAITERS_COUNT_MASK = 0xFFFF0000FFFFFFFFL;

    // Number of CAS attempts on the first stage
    private static final int SPIN_COUNT = 32;

    // Striped on-heap locks
    private final ReentrantLock[] stripedLocks;

    // The on-heap conditions for read waiters
    private final Condition[] stripedReadConditions;

    // The on-heap conditions for write waiters
    private final Condition[] stripedWriteConditions;

    // The mask applied to the hashed lock address to get a striped lock
    private final long stripeMask;

    // The policy to customize readLock/writeLock requests fairness policy
    private final LockFairnessPolicy lockFairnessPolicy;

    HDLockManager(int stripesCount) {
        this(stripesCount, new DefaultLockFairnessPolicy());
    }

    HDLockManager(int stripesCount, LockFairnessPolicy lockFairnessPolicy) {
        if (!isPowerOfTwo(stripesCount)) {
            throw new IllegalArgumentException("Number of stripes " + stripesCount + " must be a power of 2.");
        }
        checkFairnessPolicy(lockFairnessPolicy);
        stripeMask = stripesCount - 1;
        stripedLocks = new ReentrantLock[stripesCount];
        stripedReadConditions = new Condition[stripesCount];
        stripedWriteConditions = new Condition[stripesCount];

        for (int i = 0; i < stripesCount; i++) {
            ReentrantLock lock = new ReentrantLock();
            stripedLocks[i] = lock;
            stripedReadConditions[i] = lock.newCondition();
            stripedWriteConditions[i] = lock.newCondition();
        }
        this.lockFairnessPolicy = lockFairnessPolicy;
    }

    private void checkFairnessPolicy(LockFairnessPolicy lockFairnessPolicy) {
        float readLockRequestWinsPercentage = lockFairnessPolicy.readLockRequestWinsPercentage();
        float writeLockRequestWinsPercentage = lockFairnessPolicy.writeLockRequestWinsPercentage();
        float writeWaiterWinsPercentage = lockFairnessPolicy.writeWaiterWinsPercentage();
        if (!checkPercentage(readLockRequestWinsPercentage)
                || !checkPercentage(writeLockRequestWinsPercentage)
                || !checkPercentage(writeWaiterWinsPercentage)) {
            throw new IllegalArgumentException("Invalid lock fairness policy. "
                    + "Read lock request wins percentage " + readLockRequestWinsPercentage
                    + "Write lock request wins percentage " + writeLockRequestWinsPercentage
                    + "Write waiter wins percentage " + writeWaiterWinsPercentage
            );
        }
    }

    @Override
    public void readLock(long lockAddr) {
        acquireLock(lockAddr, true);
    }

    private boolean tryLock(long lockAddr, boolean sharedAccess) {
        long lockState = getLockState(lockAddr);
        // Even if there are read/write waiters, we prioritize this request, because
        // it is issued in very specific scenarios like empty node remove
        // and its failure causes new top-down B+tree traversal
        return isCompatible(lockState, sharedAccess)
                && tryIncrementUsersCount(lockAddr, lockState, sharedAccess);
    }

    @Override
    public boolean tryReadLock(long lockAddr) {
        return tryLock(lockAddr, true);
    }

    @Override
    public void instantDurationReadLock(long lockAddr) {
        instantDurationLock(lockAddr, true);
    }

    @Override
    public void writeLock(long lockAddr) {
        acquireLock(lockAddr, false);
    }

    @Override
    public boolean tryUpgradeToWriteLock(long lockAddr) {

        // Even if there are read/write waiters, we prioritize this request, because
        // it is issued in very specific scenarios like node split
        // and its failure causes new top-down B+tree traversal

        for (int i = 0; i < SPIN_COUNT; i++) {
            long lockState = getLockState(lockAddr);

            assert getUsersCount(lockState) >= 1;
            if (getUsersCount(lockState) == 1) {
                if (tryIncrementUsersCount(lockAddr, lockState, false)) {
                    return true;
                } else {
                    i--;
                }
            }
        }

        return false;
    }

    @Override
    public boolean tryWriteLock(long lockAddr) {
        return tryLock(lockAddr, false);
    }

    private void instantDurationLock(long lockAddr, boolean sharedAccess) {
        int stripeIndex = getStripe(lockAddr);
        ReentrantLock stripedLock = stripedLocks[stripeIndex];
        boolean interrupted = false;

        stripedLock.lock();
        try {
            long lockState = incrementWaitersCount(lockAddr, sharedAccess);
            while (true) {
                if (!isCompatible(lockState, sharedAccess)) {
                    Condition condition = sharedAccess ? stripedReadConditions[stripeIndex]
                            : stripedWriteConditions[stripeIndex];
                    try {
                        condition.await();
                    } catch (InterruptedException e) {
                        interrupted = true;
                    }
                } else {
                    long newLockState = updateWaitersCount0(lockState, sharedAccess, false);
                    if (AMEM.compareAndSwapLong(lockAddr, lockState, newLockState)) {
                        /**
                         * Though, the instant duration lock doesn't increment the user's count,
                         * we still have to notify waiters, because the waiters list has changed.
                         */
                        notifyWaiters(lockAddr, newLockState);
                        break;
                    }
                }
                lockState = getLockState(lockAddr);
            }
        } finally {
            stripedLock.unlock();
            if (interrupted) {
                selfInterrupt();
            }
        }
    }

    @Override
    public void instantDurationWriteLock(long lockAddr) {
        instantDurationLock(lockAddr, false);
    }

    @SuppressWarnings("checkstyle:magicnumber")
    private void acquireLock(long lockAddr, boolean sharedAccess) {

        int spinResult = tryAcquireSpin(lockAddr, sharedAccess);
        if (spinResult < 0) {
            // The flag indicates whether the request can skip await() on condition if
            // the requested lock is compatible
            boolean blockRequest = spinResult == -2;
            acquireLock0(lockAddr, blockRequest, sharedAccess);
        }
    }

    /**
     * @return -2 if the lock request was not granted and it should block,
     * -1 if the lock request was not granted but can immediately get if through the slow-path,
     * 0 if the lock request was granted.
     */
    @SuppressWarnings("checkstyle:magicnumber")
    private int tryAcquireSpin(long lockAddr, boolean sharedAccess) {
        long lockState = getLockState(lockAddr);

        if (requestShouldBlock(lockState, sharedAccess)) {
            return -2;
        }

        for (int i = 0; i < SPIN_COUNT; i++) {

            if (isCompatible(lockState, sharedAccess)) {
                if (tryIncrementUsersCount(lockAddr, lockState, sharedAccess)) {
                    return 0;
                }
            }

            lockState = getLockState(lockAddr);
        }
        return -1;
    }

    private boolean requestShouldBlock(long lockState, boolean sharedAccess) {
        if (sharedAccess && hasWaiters(lockState) && readerShouldBlock()) {
            return true;
        }
        if (!sharedAccess && hasWaiters(lockState) && writerShouldBlock()) {
            return true;
        }
        return false;
    }

    private void acquireLock0(long lockAddr, boolean blockRequest, boolean sharedAccess) {
        int stripeIndex = getStripe(lockAddr);
        ReentrantLock stripedLock = stripedLocks[stripeIndex];
        boolean interrupted = false;

        stripedLock.lock();
        try {

            long lockState = incrementWaitersCount(lockAddr, sharedAccess);
            // Don't try to take the lock without await() on condition if there are no other waiters
            boolean blockOnFirstAttempt = blockRequest && waitersCount(lockState) > 1;
            while (true) {
                if (!isCompatible(lockState, sharedAccess) || blockOnFirstAttempt) {
                    interrupted = false;
                    Condition condition = sharedAccess ? stripedReadConditions[stripeIndex]
                            : stripedWriteConditions[stripeIndex];
                    try {
                        condition.await();
                    } catch (InterruptedException e) {
                        interrupted = true;
                    }
                    blockOnFirstAttempt = false;
                } else {
                    if (tryIncrementUsersCountAndDecrementWaitersCount(lockAddr, lockState, sharedAccess)) {
                        break;
                    }
                }
                lockState = getLockState(lockAddr);
            }
        } finally {
            stripedLock.unlock();
            if (interrupted) {
                selfInterrupt();
            }
        }
    }

    @Override
    public void releaseLock(long lockAddr) {

        while (true) {
            long oldLockState = getLockState(lockAddr);

            int usersCount = getUsersCount(oldLockState);

            assert usersCount > 0 || usersCount == -1;

            boolean sharedAccess = usersCount > 0 ? true : false;

            long newLockState = decrementUsersCount0(oldLockState, sharedAccess);

            if (AMEM.compareAndSwapLong(lockAddr, oldLockState, newLockState)) {
                notifyWaiters(lockAddr, newLockState);
                return;
            }
        }
    }

    private long incrementWaitersCount(long lockAddr, boolean sharedAccess) {
        while (true) {
            long lockState = getLockState(lockAddr);

            long newLockState = updateWaitersCount0(lockState, sharedAccess, true);

            if (AMEM.compareAndSwapLong(lockAddr, lockState, newLockState)) {
                return newLockState;
            }
        }
    }

    @SuppressWarnings("checkstyle:magicnumber")
    private long updateWaitersCount0(long lockState, boolean sharedAccess, boolean increment) {
        int shiftBits = sharedAccess ? 16 : 32;
        long clearingMask = sharedAccess ? CLEARING_READ_WAITERS_COUNT_MASK : CLEARING_WRITE_WAITERS_COUNT_MASK;
        int waitersCount = getWaitersCount(lockState, sharedAccess);
        long newWaitersCount = increment ? waitersCount + 1 : waitersCount - 1;
        assert newWaitersCount >= 0;
        if (newWaitersCount > MAX_WAITERS_COUNT) {
            throw new BPlusTreeLimitException("B+tree lock reached the maximum waiters's count limit " + MAX_WAITERS_COUNT);
        }
        return (lockState & clearingMask) | newWaitersCount << shiftBits;
    }

    @SuppressWarnings("checkstyle:magicnumber")
    static int getUsersCount(long lockState) {
        // Users count can be negative, don't treat it as unsigned 2 bytes integer
        return (short) (lockState & 0xFFFFL);
    }

    private boolean tryIncrementUsersCount(long lockAddr, long oldLockState, boolean sharedAccess) {
        long newLockState = incrementUsersCount0(oldLockState, sharedAccess);
        return AMEM.compareAndSwapLong(lockAddr, oldLockState, newLockState);
    }

    @SuppressWarnings("checkstyle:magicnumber")
    private long decrementUsersCount0(long lockState, boolean sharedAccess) {
        int usersCount = getUsersCount(lockState);
        long newUsersCount = sharedAccess ? usersCount - 1 : 0;
        assert newUsersCount >= 0;
        return (lockState & CLEARING_USERS_COUNT_MASK) | (newUsersCount & 0xFFFFL);
    }

    @SuppressWarnings("checkstyle:magicnumber")
    private long incrementUsersCount0(long lockState, boolean sharedAccess) {
        long newUsersCount = sharedAccess ? getUsersCount(lockState) + 1 : -1;
        if (newUsersCount > MAX_USERS_COUNT) {
            throw new BPlusTreeLimitException("B+tree lock reached the maximum user's count limit " + MAX_USERS_COUNT);
        }
        return (lockState & CLEARING_USERS_COUNT_MASK) | (newUsersCount & 0xFFFFL);
    }

    private boolean tryIncrementUsersCountAndDecrementWaitersCount(long lockAddr, long oldLockState, boolean sharedAccess) {
        long newLockState = updateWaitersCount0(oldLockState, sharedAccess, false);
        newLockState = incrementUsersCount0(newLockState, sharedAccess);
        return AMEM.compareAndSwapLong(lockAddr, oldLockState, newLockState);
    }

    static int getReadWaitersCount(long lockState) {
        return getWaitersCount(lockState, true);
    }

    static int getWriteWaitersCount(long lockState) {
        return getWaitersCount(lockState, false);
    }

    @SuppressWarnings("checkstyle:magicnumber")
    private static int getWaitersCount(long lockState, boolean read) {
        int shiftBits = read ? 16 : 32;
        return (int) ((lockState >> shiftBits) & 0xFFFFL);
    }

    private long getLockState(long lockAddr) {
        return AMEM.getLongVolatile(lockAddr);
    }

    private boolean hasWaiters(long lockState) {
        return getReadWaitersCount(lockState) > 0 || getWriteWaitersCount(lockState) > 0;
    }

    private int waitersCount(long lockState) {
        return getReadWaitersCount(lockState) + getWriteWaitersCount(lockState);
    }

    private boolean isCompatible(long lockState, boolean sharedAccess) {
        int usersCount = getUsersCount(lockState);
        return sharedAccess ? usersCount >= 0 : usersCount == 0;
    }

    private void notifyWaiters(long lockAddr, long lockState) {
        int newReadWaitersCount = getReadWaitersCount(lockState);
        int newWriteWaitersCount = getWriteWaitersCount(lockState);
        int newUsersCount = getUsersCount(lockState);

        if (newUsersCount == 0) {
            if (newReadWaitersCount > 0 && newWriteWaitersCount > 0) {
                boolean notifyWriter = nextNotifyWriter();
                notifyWaiters0(lockAddr, notifyWriter);
            } else if (newWriteWaitersCount > 0) {
                notifyWaiters0(lockAddr, true);
            } else if (newReadWaitersCount > 0) {
                notifyWaiters0(lockAddr, false);
            }
        }

    }

    boolean readerShouldBlock() {
        float readLockRequestWinsPercentage = lockFairnessPolicy.readLockRequestWinsPercentage();
        return !fullPercentage(readLockRequestWinsPercentage) && nextPercentage() >= readLockRequestWinsPercentage;
    }

    boolean writerShouldBlock() {
        float writeLockRequestWinsPercentage = lockFairnessPolicy.writeLockRequestWinsPercentage();
        return !fullPercentage(writeLockRequestWinsPercentage) && nextPercentage() >= writeLockRequestWinsPercentage;
    }

    boolean nextNotifyWriter() {
        float writeWaiterWinsPercentage = lockFairnessPolicy.writeWaiterWinsPercentage();
        return fullPercentage(writeWaiterWinsPercentage) || nextPercentage() < writeWaiterWinsPercentage;
    }

    private void notifyWaiters0(long lockAddr, boolean writers) {
        int stripeIndex = getStripe(lockAddr);
        ReentrantLock stripedLock = stripedLocks[stripeIndex];

        Condition condition = writers ? stripedWriteConditions[stripeIndex]
                : stripedReadConditions[stripeIndex];

        stripedLock.lock();
        try {
            condition.signalAll();
        } finally {
            stripedLock.unlock();
        }
    }

    private int getStripe(long lockAddr) {
        return (int) (fastLongMix(lockAddr) & stripeMask);
    }

    private static void selfInterrupt() {
        Thread.currentThread().interrupt();
    }

    @SuppressWarnings("checkstyle:magicnumber")
    private int nextPercentage() {
        return ThreadLocalRandom.current().nextInt(100);
    }

    @SuppressWarnings("checkstyle:magicnumber")
    private boolean fullPercentage(float percentage) {
        return percentage == 100.0f;
    }

    @SuppressWarnings("checkstyle:magicnumber")
    private boolean checkPercentage(float percentage) {
        return percentage >= 0.0f && percentage <= 100.0f;
    }
}
