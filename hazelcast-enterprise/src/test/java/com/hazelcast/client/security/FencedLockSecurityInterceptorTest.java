package com.hazelcast.client.security;

import com.hazelcast.config.Config;
import com.hazelcast.cp.internal.datastructures.lock.RaftLockService;
import com.hazelcast.cp.lock.FencedLock;
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
public class FencedLockSecurityInterceptorTest extends InterceptorTestSupport {

    private String objectName;
    private FencedLock lock;

    @Before
    public void setup() {
        Config config = createConfig();
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);

        objectName = randomString();
        lock = client.getCPSubsystem().getLock(objectName);
    }

    @Override
    Config createConfig() {
        Config config = super.createConfig();
        config.getCPSubsystemConfig().setCPMemberCount(3);
        return config;
    }

    @Test
    public void isLocked() {
        interceptor.setExpectation(getObjectType(), objectName, "getLockOwnershipState");
        lock.isLocked();
    }

    @Test
    public void isLockedByCurrentThread() {
        interceptor.setExpectation(getObjectType(), objectName, "getLockOwnershipState");
        lock.isLockedByCurrentThread();
    }

    @Test
    public void getLockCount() {
        interceptor.setExpectation(getObjectType(), objectName, "getLockOwnershipState");
        lock.getLockCount();
    }

    @Test
    public void getFence() {
        lock.lock();
        interceptor.setExpectation(getObjectType(), objectName, "getLockOwnershipState");
        lock.getFence();
    }

    @Test
    public void lock() {
        interceptor.setExpectation(getObjectType(), objectName, "lock");
        lock.lock();
    }

    @Test
    public void lockAndGetFence() {
        interceptor.setExpectation(getObjectType(), objectName, "lock");
        lock.lock();
    }

    @Test
    public void unlock() {
        lock.lock();
        interceptor.setExpectation(getObjectType(), objectName, "unlock");
        lock.unlock();
    }

    @Test
    public void tryLock() {
        interceptor.setExpectation(getObjectType(), objectName, "tryLock", 0L, TimeUnit.MILLISECONDS);
        lock.tryLock();
    }

    @Test
    public void tryLock_withTimeout() {
        final long ttl = randomLong();
        interceptor.setExpectation(getObjectType(), objectName, "tryLock", ttl, TimeUnit.MILLISECONDS);
        lock.tryLock(ttl, TimeUnit.MILLISECONDS);
    }

    @Test
    public void tryLockAndGetFence() {
        interceptor.setExpectation(getObjectType(), objectName, "tryLock", 0L, TimeUnit.MILLISECONDS);
        lock.tryLockAndGetFence();
    }

    @Test
    public void tryLockAndGetFence_withTimeout() {
        final long ttl = randomLong();
        interceptor.setExpectation(getObjectType(), objectName, "tryLock", ttl, TimeUnit.MILLISECONDS);
        lock.tryLockAndGetFence(ttl, TimeUnit.MILLISECONDS);
    }

    @Override
    String getObjectType() {
        return RaftLockService.SERVICE_NAME;
    }
}
