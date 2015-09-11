package com.hazelcast.client.security;

import com.hazelcast.concurrent.atomiclong.AtomicLongService;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class AtomicLongSecurityInterceptorTest extends BaseInterceptorTest {

    String objectName;
    IAtomicLong atomicLong;

    @Before
    public void setup() {
        objectName = randomString();
        atomicLong = client.getAtomicLong(objectName);
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
        interceptor.setExpectation(getObjectType(), objectName, "alter", dummyFunction);
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
        interceptor.setExpectation(getObjectType(), objectName, "decrementAndGet");
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
        interceptor.setExpectation(getObjectType(), objectName, "incrementAndGet");
        atomicLong.incrementAndGet();
    }

    @Test
    public void getAndIncrement() {
        interceptor.setExpectation(getObjectType(), objectName, "getAndIncrement");
        atomicLong.getAndIncrement();
    }

    @Test
    public void set() {
        final long count = randomLong();
        interceptor.setExpectation(getObjectType(), objectName, "set", count);
        atomicLong.set(count);
    }

    @Override
    String getObjectType() {
        return AtomicLongService.SERVICE_NAME;
    }

}
