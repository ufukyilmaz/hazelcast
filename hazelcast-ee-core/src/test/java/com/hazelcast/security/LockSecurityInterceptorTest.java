package com.hazelcast.security;

import com.hazelcast.concurrent.lock.LockService;
import com.hazelcast.core.ILock;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
public class LockSecurityInterceptorTest extends BaseInterceptorTest {

    @Test
    public void isLocked() {
        getLock().isLocked();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "isLocked");
    }

    @Test
    public void isLockedByCurrentThread() {
        getLock().isLockedByCurrentThread();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "isLockedByCurrentThread");
    }

    @Test
    public void getLockCount() {
        getLock().getLockCount();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "getLockCount");
    }

    @Test
    public void getRemainingLeaseTime() {
        getLock().getRemainingLeaseTime();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "getRemainingLeaseTime");
    }

    @Test
    public void test1_lock() {
        getLock().lock();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "lock");
    }

    @Test
    public void test2_lock() {
        final long ttl = randomLong();
        getLock().lock(ttl, TimeUnit.MILLISECONDS);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "lock", ttl, TimeUnit.MILLISECONDS);
    }

    @Test
    public void forceUnlock() {
        getLock().forceUnlock();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "forceUnlock");
    }

    @Test
    public void unlock() {
        final ILock lock = getLock();
        lock.lock();
        interceptor.reset();;

        lock.unlock();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "unlock");
    }

    @Test
    public void test1_tryLock() {
        getLock().tryLock();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "tryLock");
    }

    @Test
    public void test2_tryLock() throws InterruptedException {
        final long ttl = randomLong();
        getLock().tryLock(ttl, TimeUnit.MILLISECONDS);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "tryLock", ttl, TimeUnit.MILLISECONDS);
    }


    ILock getLock() {
        return client.getLock(randomString());
    }

    @Override
    String getServiceName() {
        return LockService.SERVICE_NAME;
    }
}
