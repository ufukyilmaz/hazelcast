package com.hazelcast.enterprise;

import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.runner.Runner;
import org.junit.runners.model.InitializationError;
import org.junit.runners.parameterized.ParametersRunnerFactory;

import static com.hazelcast.enterprise.SampleLicense.UNLIMITED_LICENSE;

/**
 * {@link ParametersRunnerFactory} implementation which creates {@link HazelcastSerialClassRunner}
 * to run test methods serially.
 * <p>
 * See {@link com.hazelcast.test package documentation} for runners overview.
 */
public class EnterpriseSerialParametersRunnerFactory extends HazelcastParametersRunnerFactory {

    @Override
    protected Runner getClassRunner(Class<?> testClass, Object[] parameters, String testName)
            throws InitializationError {
        ClusterProperty.ENTERPRISE_LICENSE_KEY.setSystemProperty(UNLIMITED_LICENSE);
        return new EnterpriseSerialJUnitClassRunner(testClass, parameters, testName);
    }
}
