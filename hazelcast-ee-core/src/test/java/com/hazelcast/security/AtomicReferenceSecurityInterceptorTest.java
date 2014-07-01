package com.hazelcast.security;

import com.hazelcast.concurrent.atomicreference.AtomicReferenceService;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
public class AtomicReferenceSecurityInterceptorTest extends BaseInterceptorTest {

    @Test
    public void apply() {
        final DummyFunction dummyFunction = new DummyFunction(randomLong());
        getAtomicReference().apply(dummyFunction);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "apply", dummyFunction);
    }

    @Test
    public void alter() {
        final DummyFunction dummyFunction = new DummyFunction(randomLong());
        getAtomicReference().alter(dummyFunction);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "alter", dummyFunction);
    }

    @Test
    public void alterAndGet() {
        final DummyFunction dummyFunction = new DummyFunction(randomLong());
        getAtomicReference().alterAndGet(dummyFunction);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "alterAndGet", dummyFunction);
    }

    @Test
    public void getAndAlter() {
        final DummyFunction dummyFunction = new DummyFunction(randomLong());
        getAtomicReference().getAndAlter(dummyFunction);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "getAndAlter", dummyFunction);
    }

    @Test
    public void compareAndSet() {
        final String expected = randomString();
        final String update = randomString();
        getAtomicReference().compareAndSet(expected, update);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "compareAndSet", expected, update);
    }

    @Test
    public void contains() {
        final String value = randomString();
        getAtomicReference().contains(value);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "contains", value);
    }

    @Test
    public void get() {
        getAtomicReference().get();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "get");
    }

    @Test
    public void set() {
        final String value = randomString();
        getAtomicReference().set(value);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "set", value);
    }

    @Test
    public void clear() {
        getAtomicReference().clear();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "clear");
    }

    @Test
    public void getAndSet() {
        final String value = randomString();
        getAtomicReference().getAndSet(value);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "getAndSet", value);
    }

    @Test
    public void isNull() {
        getAtomicReference().isNull();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "isNull");
    }

    @Override
    String getServiceName() {
        return AtomicReferenceService.SERVICE_NAME;
    }

    IAtomicReference getAtomicReference() {
        return client.getAtomicReference(randomString());
    }
}
