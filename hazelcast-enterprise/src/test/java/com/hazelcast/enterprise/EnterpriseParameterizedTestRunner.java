package com.hazelcast.enterprise;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestRunner;
import org.junit.runners.model.InitializationError;

/**
 * Enterprise extension of {@link HazelcastTestRunner}.
 *
 * Creates {@link EnterpriseParallelJUnitClassRunner} if the test class has
 * {@link com.hazelcast.test.annotation.RunParallel} annotation, creates
 * {@link EnterpriseSerialJUnitClassRunner} otherwise.
 *
 */
public class EnterpriseParameterizedTestRunner extends HazelcastTestRunner {

    static {
        System.setProperty("hazelcast.memory.pageLookupLength", String.valueOf(1 << 16));
    }

    public EnterpriseParameterizedTestRunner(Class<?> clazz) throws Throwable {
        super(clazz);
    }

    @Override
    protected HazelcastParallelClassRunner createParallelRunner(Object[] parametersOfSingleTest, String name) throws InitializationError {
        return new EnterpriseParallelJUnitClassRunner(getTestClass().getJavaClass(), parametersOfSingleTest, name);
    }

    @Override
    protected HazelcastSerialClassRunner createSerialRunner(Object[] parametersOfSingleTest, String name) throws InitializationError {
        return new EnterpriseSerialJUnitClassRunner(getTestClass().getJavaClass(), parametersOfSingleTest, name);
    }
}
