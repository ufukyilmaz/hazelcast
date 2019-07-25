package com.hazelcast.client.security;

import com.hazelcast.config.Config;
import com.hazelcast.cp.IAtomicReference;
import com.hazelcast.cp.internal.datastructures.atomicref.RaftAtomicRefService;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AtomicReferenceSecurityInterceptorTest extends InterceptorTestSupport {

    private String objectName;
    private IAtomicReference atomicReference;

    @Before
    public void setup() {
        Config config = createConfig();
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);

        objectName = randomString();
        atomicReference = client.getCPSubsystem().getAtomicReference(objectName);
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
        atomicReference.apply(dummyFunction);
    }

    @Test
    public void alter() {
        final DummyFunction dummyFunction = new DummyFunction(randomLong());
        interceptor.setExpectation(getObjectType(), objectName, "alter", dummyFunction);
        atomicReference.alter(dummyFunction);
    }

    @Test
    public void alterAndGet() {
        final DummyFunction dummyFunction = new DummyFunction(randomLong());
        interceptor.setExpectation(getObjectType(), objectName, "alterAndGet", dummyFunction);
        atomicReference.alterAndGet(dummyFunction);
    }

    @Test
    public void getAndAlter() {
        final DummyFunction dummyFunction = new DummyFunction(randomLong());
        interceptor.setExpectation(getObjectType(), objectName, "getAndAlter", dummyFunction);
        atomicReference.getAndAlter(dummyFunction);
    }

    @Test
    public void compareAndSet() {
        final String expected = randomString();
        final String update = randomString();
        interceptor.setExpectation(getObjectType(), objectName, "compareAndSet", expected, update);
        atomicReference.compareAndSet(expected, update);
    }

    @Test
    public void contains() {
        final String value = randomString();
        interceptor.setExpectation(getObjectType(), objectName, "contains", value);
        atomicReference.contains(value);
    }

    @Test
    public void get() {
        interceptor.setExpectation(getObjectType(), objectName, "get");
        atomicReference.get();
    }

    @Test
    public void set() {
        final String value = randomString();
        interceptor.setExpectation(getObjectType(), objectName, "set", value);
        atomicReference.set(value);
    }

    @Test
    public void clear() {
        interceptor.setExpectation(getObjectType(), objectName, "set", (Object) null);
        atomicReference.clear();
    }

    @Test
    public void getAndSet() {
        final String value = randomString();
        interceptor.setExpectation(getObjectType(), objectName, "getAndSet", value);
        atomicReference.getAndSet(value);
    }

    @Test
    public void isNull() {
        interceptor.setExpectation(getObjectType(), objectName, "contains", (Object) null);
        atomicReference.isNull();
    }

    @Override
    String getObjectType() {
        return RaftAtomicRefService.SERVICE_NAME;
    }
}
