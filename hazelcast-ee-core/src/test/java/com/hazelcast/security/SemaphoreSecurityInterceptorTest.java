package com.hazelcast.security;

import com.hazelcast.concurrent.semaphore.SemaphoreService;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.ProblematicTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
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
        interceptor.reset();
    }

    @Test
    public void init() {
        final int permit = randomInt(100);
        semaphore.init(permit);
        interceptor.assertMethod(getObjectType(), objectName, "init", permit);
    }

    @Test
    public void test1_acquire() throws InterruptedException {
        semaphore.acquire();
        interceptor.assertMethod(getObjectType(), objectName, "acquire");
    }

    @Test
    public void test2_acquire() throws InterruptedException {
        final int permit = randomInt(100);
        semaphore.acquire(permit);
        interceptor.assertMethod(getObjectType(), objectName, "acquire", permit);
    }

    @Test
    public void availablePermits() {
        semaphore.availablePermits();
        interceptor.assertMethod(getObjectType(), objectName, "availablePermits");
    }

    @Test
    public void drainPermits() {
        semaphore.drainPermits();
        interceptor.assertMethod(getObjectType(), objectName, "drainPermits");
    }

    @Test
    public void reducePermits() {
        final int permit = randomInt(100);
        semaphore.reducePermits(permit);
        interceptor.assertMethod(getObjectType(), objectName, "reducePermits", permit);
    }

    @Test
    public void test1_release() {
        semaphore.release();
        interceptor.assertMethod(getObjectType(), objectName, "release");
    }

    @Test
    public void test2_release() {
        final int permit = randomInt(100);
        semaphore.release(permit);
        interceptor.assertMethod(getObjectType(), objectName, "release", permit);
    }

    @Test
    public void test1_tryAcquire() {
        semaphore.tryAcquire();
        interceptor.assertMethod(getObjectType(), objectName, "tryAcquire");
    }

    @Test
    @Category(ProblematicTest.class)
    public void test2_tryAcquire() {
        final int permit = randomInt(100);
        semaphore.tryAcquire(permit);
        interceptor.assertMethod(getObjectType(), objectName, "tryAcquire", permit);
    }

    @Test
    public void test3_tryAcquire() throws InterruptedException {
        final long timeout = randomLong();
        semaphore.tryAcquire(timeout, TimeUnit.MILLISECONDS);
        interceptor.assertMethod(getObjectType(), objectName, "tryAcquire", timeout, TimeUnit.MILLISECONDS);
    }

    @Test
    @Category(ProblematicTest.class)
    public void test4_tryAcquire() throws InterruptedException {
        final int permit = randomInt(100);
        final long timeout = randomLong();
        semaphore.tryAcquire(permit, timeout, TimeUnit.MILLISECONDS);
        interceptor.assertMethod(getObjectType(), objectName, "tryAcquire", permit, timeout, TimeUnit.MILLISECONDS);
    }

    @Override
    String getObjectType() {
        return SemaphoreService.SERVICE_NAME;
    }

}
