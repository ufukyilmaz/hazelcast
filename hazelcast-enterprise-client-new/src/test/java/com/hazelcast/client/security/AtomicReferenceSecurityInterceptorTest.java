package com.hazelcast.client.security;

import com.hazelcast.concurrent.atomicreference.AtomicReferenceService;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
public class AtomicReferenceSecurityInterceptorTest extends BaseInterceptorTest {

    String objectName;
    IAtomicReference atomicReference;

    @Before
    public void setup() {
        objectName = randomString();
        atomicReference = client.getAtomicReference(objectName);
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
        interceptor.setExpectation(getObjectType(), objectName, "clear");
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
        interceptor.setExpectation(getObjectType(), objectName, "isNull");
        atomicReference.isNull();
    }

    @Override
    String getObjectType() {
        return AtomicReferenceService.SERVICE_NAME;
    }
}
