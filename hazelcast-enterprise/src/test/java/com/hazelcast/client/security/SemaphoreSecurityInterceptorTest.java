package com.hazelcast.client.security;

import com.hazelcast.config.Config;
import com.hazelcast.config.cp.CPSemaphoreConfig;
import com.hazelcast.cp.ISemaphore;
import com.hazelcast.cp.internal.datastructures.semaphore.RaftSemaphoreService;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SemaphoreSecurityInterceptorTest extends InterceptorTestSupport {

    private String objectName = randomString();
    private ISemaphore semaphore;

    @Before
    public void setup() {
        Config config = createConfig();
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);

        semaphore = client.getCPSubsystem().getSemaphore(objectName);
        semaphore.init(100);
    }

    @Override
    Config createConfig() {
        Config config = super.createConfig();
        config.getCPSubsystemConfig().setCPMemberCount(3)
            .addSemaphoreConfig(new CPSemaphoreConfig(objectName).setJDKCompatible(true));
        return config;
    }

    @Test
    public void init() {
        final int permit = randomInt(100) + 1;
        interceptor.setExpectation(getObjectType(), objectName, "init", permit);
        semaphore.init(permit);
    }

    @Test
    public void acquire() throws InterruptedException {
        interceptor.setExpectation(getObjectType(), objectName, "acquire", 1);
        semaphore.acquire();
    }

    @Test
    public void acquire_multiplePermits() throws InterruptedException {
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
    public void increasePermits() {
        final int permit = randomInt(100) + 1;
        interceptor.setExpectation(getObjectType(), objectName, "increasePermits", permit);
        semaphore.increasePermits(permit);
    }

    @Test
    public void release() {
        interceptor.setExpectation(getObjectType(), objectName, "release", 1);
        semaphore.release();
    }

    @Test
    public void release_multiplePermits() {
        final int permit = randomInt(100) + 1;
        interceptor.setExpectation(getObjectType(), objectName, "release", permit);
        semaphore.release(permit);
    }

    @Test
    public void tryAcquire() {
        interceptor.setExpectation(getObjectType(), objectName, "tryAcquire", 1);
        semaphore.tryAcquire();
    }

    @Test
    public void tryAcquire_multiplePermits() {
        final int permit = randomInt(100) + 1;
        interceptor.setExpectation(getObjectType(), objectName, "tryAcquire", permit);
        semaphore.tryAcquire(permit);
    }

    @Test
    public void tryAcquire_withTimeout() throws InterruptedException {
        final long timeout = randomLong() + 1;
        interceptor.setExpectation(getObjectType(), objectName, "tryAcquire", 1, timeout, TimeUnit.MILLISECONDS);
        semaphore.tryAcquire(timeout, TimeUnit.MILLISECONDS);
    }

    @Test
    public void tryAcquire_multiplePermits_withTimeout() throws InterruptedException {
        final int permit = randomInt(100) + 1;
        final long timeout = randomLong() + 1;
        interceptor.setExpectation(getObjectType(), objectName, "tryAcquire", permit, timeout, TimeUnit.MILLISECONDS);
        semaphore.tryAcquire(permit, timeout, TimeUnit.MILLISECONDS);
    }

    @Override
    String getObjectType() {
        return RaftSemaphoreService.SERVICE_NAME;
    }

}
