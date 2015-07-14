package com.hazelcast.client.security;

import com.hazelcast.concurrent.atomicreference.AtomicReferenceService;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
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
        atomicReference.apply(dummyFunction);
        interceptor.assertMethod(getObjectType(), objectName, "apply", dummyFunction);
    }

    @Test
    public void alter() {
        final DummyFunction dummyFunction = new DummyFunction(randomLong());
        atomicReference.alter(dummyFunction);
        interceptor.assertMethod(getObjectType(), objectName, "alter", dummyFunction);
    }

    @Test
    public void alterAndGet() {
        final DummyFunction dummyFunction = new DummyFunction(randomLong());
        atomicReference.alterAndGet(dummyFunction);
        interceptor.assertMethod(getObjectType(), objectName, "alterAndGet", dummyFunction);
    }

    @Test
    public void getAndAlter() {
        final DummyFunction dummyFunction = new DummyFunction(randomLong());
        atomicReference.getAndAlter(dummyFunction);
        interceptor.assertMethod(getObjectType(), objectName, "getAndAlter", dummyFunction);
    }

    @Test
    public void compareAndSet() {
        final String expected = randomString();
        final String update = randomString();
        atomicReference.compareAndSet(expected, update);
        interceptor.assertMethod(getObjectType(), objectName, "compareAndSet", expected, update);
    }

    @Test
    public void contains() {
        final String value = randomString();
        atomicReference.contains(value);
        interceptor.assertMethod(getObjectType(), objectName, "contains", value);
    }

    @Test
    public void get() {
        atomicReference.get();
        interceptor.assertMethod(getObjectType(), objectName, "get");
    }

    @Test
    public void set() {
        final String value = randomString();
        atomicReference.set(value);
        interceptor.assertMethod(getObjectType(), objectName, "set", value);
    }

    @Test
    public void clear() {
        atomicReference.clear();
        interceptor.assertMethod(getObjectType(), objectName, "clear");
    }

    @Test
    public void getAndSet() {
        final String value = randomString();
        atomicReference.getAndSet(value);
        interceptor.assertMethod(getObjectType(), objectName, "getAndSet", value);
    }

    @Test
    public void isNull() {
        atomicReference.isNull();
        interceptor.assertMethod(getObjectType(), objectName, "isNull");
    }

    @Override
    String getObjectType() {
        return AtomicReferenceService.SERVICE_NAME;
    }
}
