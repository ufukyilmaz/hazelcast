package com.hazelcast.security;

import com.hazelcast.concurrent.lock.LockService;
import com.hazelcast.core.ILock;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import org.junit.Before;
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
        lock.isLocked();
        interceptor.assertMethod(getObjectType(), objectName, "isLocked");
    }

    @Test
    public void isLockedByCurrentThread() {
        lock.isLockedByCurrentThread();
        interceptor.assertMethod(getObjectType(), objectName, "isLockedByCurrentThread");
    }

    @Test
    public void getLockCount() {
        lock.getLockCount();
        interceptor.assertMethod(getObjectType(), objectName, "getLockCount");
    }

    @Test
    public void getRemainingLeaseTime() {
        lock.getRemainingLeaseTime();
        interceptor.assertMethod(getObjectType(), objectName, "getRemainingLeaseTime");
    }

    @Test
    public void test1_lock() {
        lock.lock();
        interceptor.assertMethod(getObjectType(), objectName, "lock");
    }

    @Test
    public void test2_lock() {
        final long ttl = randomLong();
        lock.lock(ttl, TimeUnit.MILLISECONDS);
        interceptor.assertMethod(getObjectType(), objectName, "lock", ttl, TimeUnit.MILLISECONDS);
    }

    @Test
    public void forceUnlock() {
        lock.forceUnlock();
        interceptor.assertMethod(getObjectType(), objectName, "forceUnlock");
    }

    @Test
    public void unlock() {
        lock.lock();
        interceptor.reset();
        lock.unlock();
        interceptor.assertMethod(getObjectType(), objectName, "unlock");
    }

    @Test
    public void test1_tryLock() {
        lock.tryLock();
        interceptor.assertMethod(getObjectType(), objectName, "tryLock");
    }

    @Test
    public void test2_tryLock() throws InterruptedException {
        final long ttl = randomLong();
        lock.tryLock(ttl, TimeUnit.MILLISECONDS);
        interceptor.assertMethod(getObjectType(), objectName, "tryLock", ttl, TimeUnit.MILLISECONDS);
    }

    @Override
    String getObjectType() {
        return LockService.SERVICE_NAME;
    }
}
