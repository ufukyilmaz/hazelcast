package com.hazelcast.security;

import com.hazelcast.concurrent.semaphore.SemaphoreService;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
public class SemaphoreSecurityInterceptorTest extends BaseInterceptorTest {

    @Test
    public void init() {
        final int permit = randomInt(100);
        final ISemaphore semaphore = client.getSemaphore(randomString());
        semaphore.init(permit);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "init", permit);
    }

    @Test
    public void test1_acquire() throws InterruptedException {
        getSemaphore().acquire();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "acquire");
    }

    @Test
    public void test2_acquire() throws InterruptedException {
        final int permit = randomInt(100);
        getSemaphore().acquire(permit);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "acquire", permit);
    }

    @Test
    public void availablePermits() {
        getSemaphore().availablePermits();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "availablePermits");
    }

    @Test
    public void drainPermits() {
        getSemaphore().drainPermits();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "drainPermits");
    }

    @Test
    public void reducePermits() {
        final int permit = randomInt(100);
        getSemaphore().reducePermits(permit);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "reducePermits", permit);
    }

    @Test
    public void test1_release() {
        getSemaphore().release();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "release");
    }

    @Test
    public void test2_release() {
        final int permit = randomInt(100);
        getSemaphore().release(permit);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "release", permit);
    }

    @Test
    public void test1_tryAcquire() {
        getSemaphore().tryAcquire();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "tryAcquire");
    }

    @Test
    public void test2_tryAcquire() {
        final int permit = randomInt(100);
        getSemaphore().tryAcquire(permit);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "tryAcquire", permit);
    }

    @Test
    public void test3_tryAcquire() throws InterruptedException {
        final long timeout = randomLong();
        getSemaphore().tryAcquire(timeout, TimeUnit.MILLISECONDS);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "tryAcquire", timeout, TimeUnit.MILLISECONDS);
    }

    @Test
    public void test4_tryAcquire() throws InterruptedException {
        final int permit = randomInt(100);
        final long timeout = randomLong();
        getSemaphore().tryAcquire(permit, timeout, TimeUnit.MILLISECONDS);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "tryAcquire", permit, timeout, TimeUnit.MILLISECONDS);
    }

    @Override
    String getServiceName() {
        return SemaphoreService.SERVICE_NAME;
    }

    ISemaphore getSemaphore() {
        final ISemaphore semaphore = client.getSemaphore(randomString());
        semaphore.init(100);
        interceptor.reset();
        return semaphore;
    }
}
