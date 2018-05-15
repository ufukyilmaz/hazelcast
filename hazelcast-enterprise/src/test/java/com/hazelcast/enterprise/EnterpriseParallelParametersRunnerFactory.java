package com.hazelcast.enterprise;

import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.runners.parameterized.ParametersRunnerFactory;

/**
 * {@link ParametersRunnerFactory} implementation which creates {@link HazelcastParallelClassRunner}
 * to run test methods in parallel.
 * <p>
 * See {@link com.hazelcast.test package documentation} for runners overview.
 */
public class EnterpriseParallelParametersRunnerFactory extends EnterpriseParametersRunnerFactory {

    @Override
    protected boolean isParallel(Class<?> testClass) {
        return true;
    }
}
