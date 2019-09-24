package com.hazelcast.client.security;

import com.hazelcast.config.Config;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.cp.internal.datastructures.atomiclong.AtomicLongService;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AtomicLongSecurityInterceptorTest extends InterceptorTestSupport {

    private String objectName;
    private IAtomicLong atomicLong;

    @Before
    public void setup() {
        Config config = createConfig();
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);

        objectName = randomString();
        atomicLong = client.getCPSubsystem().getAtomicLong(objectName);
    }

    @Override
    Config createConfig() {
        Config config = super.createConfig();
        config.getCPSubsystemConfig().setCPMemberCount(3);
        return config;
    }

    @Test
    public void apply() {
        final DummyFunction dummyFunction = new DummyFunction(randomLong());
        interceptor.setExpectation(getObjectType(), objectName, "apply", dummyFunction);
        atomicLong.apply(dummyFunction);
    }

    @Test
    public void alter() {
        final DummyFunction dummyFunction = new DummyFunction(randomLong());
        interceptor.setExpectation(getObjectType(), objectName, "alterAndGet", dummyFunction);
        atomicLong.alter(dummyFunction);
    }

    @Test
    public void alterAndGet() {
        final DummyFunction dummyFunction = new DummyFunction(randomLong());
        interceptor.setExpectation(getObjectType(), objectName, "alterAndGet", dummyFunction);
        atomicLong.alterAndGet(dummyFunction);
    }

    @Test
    public void getAndAlter() {
        final DummyFunction dummyFunction = new DummyFunction(randomLong());
        interceptor.setExpectation(getObjectType(), objectName, "getAndAlter", dummyFunction);
        atomicLong.getAndAlter(dummyFunction);
    }

    @Test
    public void addAndGet() {
        final long count = randomLong();
        interceptor.setExpectation(getObjectType(), objectName, "addAndGet", count);
        atomicLong.addAndGet(count);
    }

    @Test
    public void compareAndSet() {
        final long expected = randomLong();
        final long update = randomLong();
        interceptor.setExpectation(getObjectType(), objectName, "compareAndSet", expected, update);
        atomicLong.compareAndSet(expected, update);
    }

    @Test
    public void decrementAndGet() {
        interceptor.setExpectation(getObjectType(), objectName, "addAndGet", -1L);
        atomicLong.decrementAndGet();
    }

    @Test
    public void get() {
        interceptor.setExpectation(getObjectType(), objectName, "get");
        atomicLong.get();
    }

    @Test
    public void getAndAdd() {
        final long count = randomLong();
        interceptor.setExpectation(getObjectType(), objectName, "getAndAdd", count);
        atomicLong.getAndAdd(count);
    }

    @Test
    public void getAndSet() {
        final long count = randomLong();
        interceptor.setExpectation(getObjectType(), objectName, "getAndSet", count);
        atomicLong.getAndSet(count);
    }

    @Test
    public void incrementAndGet() {
        interceptor.setExpectation(getObjectType(), objectName, "addAndGet", 1L);
        atomicLong.incrementAndGet();
    }

    @Test
    public void getAndIncrement() {
        interceptor.setExpectation(getObjectType(), objectName, "getAndAdd", 1L);
        atomicLong.getAndIncrement();
    }

    @Test
    public void set() {
        final long count = randomLong();
        interceptor.setExpectation(getObjectType(), objectName, "getAndSet", count);
        atomicLong.set(count);
    }

    @Override
    String getObjectType() {
        return AtomicLongService.SERVICE_NAME;
    }

}
