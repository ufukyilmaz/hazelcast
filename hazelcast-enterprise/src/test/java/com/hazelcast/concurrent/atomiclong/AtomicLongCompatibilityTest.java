package com.hazelcast.concurrent.atomiclong;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.starter.HazelcastStarterUtils.assertInstanceOfByClassName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Compatibility test for IAtomicLong.
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class AtomicLongCompatibilityTest extends AtomicLongAbstractTest {

    @Override
    protected HazelcastInstance[] newInstances() {
        return createHazelcastInstanceFactory().newInstances();
    }

    @Override
    public void apply_whenException() {
        atomicLong.set(1);
        try {
            atomicLong.apply(new FailingFunction());
            fail();
        } catch (Throwable t) {
            assertInstanceOfByClassName("com.hazelcast.test.ExpectedRuntimeException", t);
        }

        assertEquals(1, atomicLong.get());
    }

    @Override
    public void alter_whenException() {
        atomicLong.set(10);

        try {
            atomicLong.alter(new FailingFunction());
            fail();
        } catch (Throwable expected) {
            assertInstanceOfByClassName("com.hazelcast.test.ExpectedRuntimeException", expected);
        }

        assertEquals(10, atomicLong.get());
    }

    @Override
    public void alterAndGet_whenException() {
        atomicLong.set(10);

        try {
            atomicLong.alterAndGet(new FailingFunction());
            fail();
        } catch (Throwable expected) {
            assertInstanceOfByClassName("com.hazelcast.test.ExpectedRuntimeException", expected);
        }

        assertEquals(10, atomicLong.get());
    }

    @Override
    public void getAndAlter_whenException() {
        atomicLong.set(10);

        try {
            atomicLong.getAndAlter(new FailingFunction());
            fail();
        } catch (Throwable expected) {
            assertInstanceOfByClassName("com.hazelcast.test.ExpectedRuntimeException", expected);
        }

        assertEquals(10, atomicLong.get());
    }
}
