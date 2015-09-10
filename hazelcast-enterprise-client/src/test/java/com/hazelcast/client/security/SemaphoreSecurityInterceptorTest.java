package com.hazelcast.client.security;

import com.hazelcast.concurrent.semaphore.SemaphoreService;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.Repeat;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
public class SemaphoreSecurityInterceptorTest extends BaseInterceptorTest {

    String objectName;
    ISemaphore semaphore;

    @Before
    public void setup() {
        objectName = randomString();
        semaphore = client.getSemaphore(objectName);
        semaphore.init(100);
    }

    @Test
    public void init() {
        final int permit = randomInt(100) + 1;
        interceptor.setExpectation(getObjectType(), objectName, "init", permit);
        semaphore.init(permit);
    }

    @Test
    public void test1_acquire() throws InterruptedException {
        interceptor.setExpectation(getObjectType(), objectName, "acquire", 1);
        semaphore.acquire();
    }

    @Test
    public void test2_acquire() throws InterruptedException {
        final int permit = randomInt(100) + 1;
        interceptor.setExpectation(getObjectType(), objectName, "acquire", permit);
        semaphore.acquire(permit);
    }

    @Test
    public void availablePermits() {
        interceptor.setExpectation(getObjectType(), objectName, "availablePermits");
        semaphore.availablePermits();
    }

    @Test
    public void drainPermits() {
        interceptor.setExpectation(getObjectType(), objectName, "drainPermits");
        semaphore.drainPermits();
    }

    @Test
    public void reducePermits() {
        final int permit = randomInt(100) + 1;
        interceptor.setExpectation(getObjectType(), objectName, "reducePermits", permit);
        semaphore.reducePermits(permit);
    }

    @Test
    public void test1_release() {
        interceptor.setExpectation(getObjectType(), objectName, "release", 1);
        semaphore.release();
    }

    @Test
    public void test2_release() {
        final int permit = randomInt(100) + 1;
        interceptor.setExpectation(getObjectType(), objectName, "release", permit);
        semaphore.release(permit);
    }

    @Test
    public void test1_tryAcquire() {
        interceptor.setExpectation(getObjectType(), objectName, "tryAcquire", 1);
        semaphore.tryAcquire();
    }

    @Test
    public void test2_tryAcquire() {
        final int permit = randomInt(100) + 1;
        interceptor.setExpectation(getObjectType(), objectName, "tryAcquire", permit);
        semaphore.tryAcquire(permit);
    }

    @Test
    public void test3_tryAcquire() throws InterruptedException {
        final long timeout = randomLong() + 1;
        interceptor.setExpectation(getObjectType(), objectName, "tryAcquire", 1, timeout, TimeUnit.MILLISECONDS);
        semaphore.tryAcquire(timeout, TimeUnit.MILLISECONDS);
    }

    @Test
    public void test4_tryAcquire() throws InterruptedException {
        final int permit = randomInt(100) + 1;
        final long timeout = randomLong() + 1;
        interceptor.setExpectation(getObjectType(), objectName, "tryAcquire", permit, timeout, TimeUnit.MILLISECONDS);
        semaphore.tryAcquire(permit, timeout, TimeUnit.MILLISECONDS);
    }

    @Override
    String getObjectType() {
        return SemaphoreService.SERVICE_NAME;
    }

}
