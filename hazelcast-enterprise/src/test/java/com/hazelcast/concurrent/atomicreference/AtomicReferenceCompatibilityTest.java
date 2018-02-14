package com.hazelcast.concurrent.atomicreference;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.starter.Utils.assertInstanceOfByClassName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Compatibility test for AtomicReference.
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class AtomicReferenceCompatibilityTest extends AtomicReferenceAbstractTest {

    @Override
    protected HazelcastInstance[] newInstances() {
        return createHazelcastInstanceFactory().newInstances();
    }

    @Override
    public void apply_whenException() {
        ref.set("foo");

        try {
            ref.apply(new FailingFunction());
            fail();
        } catch (Throwable t) {
            assertInstanceOfByClassName("com.hazelcast.core.HazelcastException", t);
        }

        assertEquals("foo", ref.get());
    }

    @Override
    public void alter_whenException() {
        ref.set("foo");

        try {
            ref.alter(new FailingFunction());
            fail();
        } catch (Throwable t) {
            assertInstanceOfByClassName("com.hazelcast.core.HazelcastException", t);
        }

        assertEquals("foo", ref.get());
    }

    @Override
    public void alterAndGet_whenException() {
        ref.set("foo");

        try {
            ref.alterAndGet(new FailingFunction());
            fail();
        } catch (Throwable t) {
            assertInstanceOfByClassName("com.hazelcast.core.HazelcastException", t);
        }

        assertEquals("foo", ref.get());
    }

    @Override
    public void getAndAlter_whenException() {
        ref.set("foo");

        try {
            ref.getAndAlter(new FailingFunction());
            fail();
        } catch (Throwable t) {
            assertInstanceOfByClassName("com.hazelcast.core.HazelcastException", t);
        }

        assertEquals("foo", ref.get());
    }
}
