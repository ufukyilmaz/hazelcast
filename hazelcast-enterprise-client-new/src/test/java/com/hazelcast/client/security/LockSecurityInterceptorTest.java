package com.hazelcast.client.security;

import com.hazelcast.concurrent.lock.LockService;
import com.hazelcast.core.ICondition;
import com.hazelcast.core.ILock;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
public class LockSecurityInterceptorTest extends BaseInterceptorTest {

    String objectName;
    ILock lock;

    @Before
    public void setup() {
        objectName = randomString();
        lock = client.getLock(objectName);
    }

    @Test
    public void isLocked() {
        interceptor.setExpectation(getObjectType(), objectName, "isLocked");
        lock.isLocked();
    }

    @Test
    public void isLockedByCurrentThread() {
        interceptor.setExpectation(getObjectType(), objectName, "isLockedByCurrentThread");
        lock.isLockedByCurrentThread();
    }

    @Test
    public void getLockCount() {
        interceptor.setExpectation(getObjectType(), objectName, "getLockCount");
        lock.getLockCount();
    }

    @Test
    public void getRemainingLeaseTime() {
        interceptor.setExpectation(getObjectType(), objectName, "getRemainingLeaseTime");
        lock.getRemainingLeaseTime();
    }

    @Test
    public void test1_lock() {
        interceptor.setExpectation(getObjectType(), objectName, "lock");
        lock.lock();
    }

    @Test
    public void tesLockWithPositiveTTL() {
        final long ttl = randomLong() + 1;
        interceptor.setExpectation(getObjectType(), objectName, "lock", ttl, TimeUnit.MILLISECONDS);
        lock.lock(ttl, TimeUnit.MILLISECONDS);
    }

    @Test
    public void forceUnlock() {
        interceptor.setExpectation(getObjectType(), objectName, "forceUnlock");
        lock.forceUnlock();
    }

    @Test
    public void unlock() {
        lock.lock();
        interceptor.setExpectation(getObjectType(), objectName, "unlock");
        lock.unlock();
    }

    @Test
    public void test1_tryLock() {
        interceptor.setExpectation(getObjectType(), objectName, "tryLock", 0l, TimeUnit.MILLISECONDS);
        // lock.tryLock() == lock.tryLock(0,,TimeUnit.MILLISECONDS) so i pass those parameters into setExpectation
        lock.tryLock();
    }

    @Test
    public void test2_tryLock() throws InterruptedException {
        final long ttl = randomLong();
        interceptor.setExpectation(getObjectType(), objectName, "tryLock", ttl, TimeUnit.MILLISECONDS);
        lock.tryLock(ttl, TimeUnit.MILLISECONDS);
    }

    @Test
    public void test_condition_await() throws InterruptedException {
        ICondition iCondition = lock.newCondition(randomString());
        lock.lock();
        interceptor.setExpectation(getObjectType(), objectName, "await", 1l, TimeUnit.MILLISECONDS);
        iCondition.await(1, TimeUnit.MILLISECONDS);
    }

    @Test
    public void test_condition_signal() throws InterruptedException {
        ICondition iCondition = lock.newCondition(randomString());
        lock.lock();
        interceptor.setExpectation(getObjectType(), objectName, "signal");
        iCondition.signal();
    }

    @Test
    public void test_condition_signalAll() throws InterruptedException {
        ICondition iCondition = lock.newCondition(randomString());
        lock.lock();
        interceptor.setExpectation(getObjectType(), objectName, "signalAll");
        iCondition.signalAll();
    }

    @Override
    String getObjectType() {
        return LockService.SERVICE_NAME;
    }
}
