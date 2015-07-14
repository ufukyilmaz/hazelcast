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
        atomicLong.apply(dummyFunction);
        interceptor.assertMethod(getObjectType(), objectName, "apply", dummyFunction);
    }

    @Test
    public void alter() {
        final DummyFunction dummyFunction = new DummyFunction(randomLong());
        atomicLong.alter(dummyFunction);
        interceptor.assertMethod(getObjectType(), objectName, "alter", dummyFunction);
    }

    @Test
    public void alterAndGet() {
        final DummyFunction dummyFunction = new DummyFunction(randomLong());
        atomicLong.alterAndGet(dummyFunction);
        interceptor.assertMethod(getObjectType(), objectName, "alterAndGet", dummyFunction);
    }

    @Test
    public void getAndAlter() {
        final DummyFunction dummyFunction = new DummyFunction(randomLong());
        atomicLong.getAndAlter(dummyFunction);
        interceptor.assertMethod(getObjectType(), objectName, "getAndAlter", dummyFunction);
    }

    @Test
    public void addAndGet() {
        final long count = randomLong();
        atomicLong.addAndGet(count);
        interceptor.assertMethod(getObjectType(), objectName, "addAndGet", count);
    }

    @Test
    public void compareAndSet() {
        final long expected = randomLong();
        final long update = randomLong();
        atomicLong.compareAndSet(expected, update);
        interceptor.assertMethod(getObjectType(), objectName, "compareAndSet", expected, update);
    }

    @Test
    public void decrementAndGet() {
        atomicLong.decrementAndGet();
        interceptor.assertMethod(getObjectType(), objectName, "decrementAndGet");
    }

    @Test
    public void get() {
        atomicLong.get();
        interceptor.assertMethod(getObjectType(), objectName, "get");
    }

    @Test
    public void getAndAdd() {
        final long count = randomLong();
        atomicLong.getAndAdd(count);
        interceptor.assertMethod(getObjectType(), objectName, "getAndAdd", count);
    }

    @Test
    public void getAndSet() {
        final long count = randomLong();
        atomicLong.getAndSet(count);
        interceptor.assertMethod(getObjectType(), objectName, "getAndSet", count);
    }

    @Test
    public void incrementAndGet() {
        atomicLong.incrementAndGet();
        interceptor.assertMethod(getObjectType(), objectName, "incrementAndGet");
    }

    @Test
    public void getAndIncrement() {
        atomicLong.getAndIncrement();
        interceptor.assertMethod(getObjectType(), objectName, "getAndIncrement");
    }

    @Test
    public void set() {
        final long count = randomLong();
        atomicLong.set(count);
        interceptor.assertMethod(getObjectType(), objectName, "set", count);
    }

    @Override
    String getObjectType() {
        return AtomicLongService.SERVICE_NAME;
    }

}
