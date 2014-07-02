package com.hazelcast.security;

import com.hazelcast.concurrent.atomiclong.AtomicLongService;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
public class AtomicLongSecurityInterceptorTest extends BaseInterceptorTest {

    @Test
    public void apply() {
        final DummyFunction dummyFunction = new DummyFunction(randomLong());
        getAtomicLong().apply(dummyFunction);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "apply", dummyFunction);
    }

    @Test
    public void alter() {
        final DummyFunction dummyFunction = new DummyFunction(randomLong());
        getAtomicLong().alter(dummyFunction);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "alter", dummyFunction);
    }

    @Test
    public void alterAndGet() {
        final DummyFunction dummyFunction = new DummyFunction(randomLong());
        getAtomicLong().alterAndGet(dummyFunction);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "alterAndGet", dummyFunction);
    }

    @Test
    public void getAndAlter() {
        final DummyFunction dummyFunction = new DummyFunction(randomLong());
        getAtomicLong().getAndAlter(dummyFunction);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "getAndAlter", dummyFunction);
    }

    @Test
    public void addAndGet() {
        final long count = randomLong();
        getAtomicLong().addAndGet(count);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "addAndGet", count);
    }

    @Test
    public void compareAndSet() {
        final long expected = randomLong();
        final long update = randomLong();
        getAtomicLong().compareAndSet(expected, update);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "compareAndSet", expected, update);
    }

    @Test
    public void decrementAndGet() {
        getAtomicLong().decrementAndGet();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "decrementAndGet");
    }

    @Test
    public void get() {
        getAtomicLong().get();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "get");
    }

    @Test
    public void getAndAdd() {
        final long count = randomLong();
        getAtomicLong().getAndAdd(count);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "getAndAdd", count);
    }

    @Test
    public void getAndSet() {
        final long count = randomLong();
        getAtomicLong().getAndSet(count);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "getAndSet", count);
    }

    @Test
    public void incrementAndGet() {
        getAtomicLong().incrementAndGet();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "incrementAndGet");
    }

    @Test
    public void getAndIncrement() {
        getAtomicLong().getAndIncrement();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "getAndIncrement");
    }

    @Test
    public void set() {
        final long count = randomLong();
        getAtomicLong().set(count);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "set", count);
    }

    @Override
    String getServiceName() {
        return AtomicLongService.SERVICE_NAME;
    }

    IAtomicLong getAtomicLong() {
        return client.getAtomicLong(randomString());
    }
}
