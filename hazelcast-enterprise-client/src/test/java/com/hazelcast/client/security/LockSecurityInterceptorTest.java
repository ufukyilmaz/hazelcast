package com.hazelcast.client.security;

import com.hazelcast.concurrent.lock.LockService;
import com.hazelcast.core.ILock;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
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
    public void tesLockWithPositiveTTL() {
        final long ttl = randomLong() + 1;
        lock.lock(ttl, TimeUnit.MILLISECONDS);
        interceptor.assertMethod(getObjectType(), objectName, "lock", ttl, TimeUnit.MILLISECONDS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLockWithZeroTTL() {
        final long ttl = 0;
        lock.lock(ttl, TimeUnit.MILLISECONDS);
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
        // lock.tryLock() == lock.tryLock(0,,TimeUnit.MILLISECONDS) so i pass those parameters into assertMethod
        interceptor.assertMethod(getObjectType(), objectName, "tryLock",0l,TimeUnit.MILLISECONDS);
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
