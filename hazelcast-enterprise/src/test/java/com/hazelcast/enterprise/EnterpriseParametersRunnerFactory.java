package com.hazelcast.enterprise;

import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.runners.model.InitializationError;

import static com.hazelcast.enterprise.SampleLicense.UNLIMITED_LICENSE;

/**
 * Enterprise extension of {@link HazelcastParametersRunnerFactory}.
 *
 * Creates {@link EnterpriseParallelJUnitClassRunner} if the test class has
 * {@link com.hazelcast.test.annotation.ParallelTest} category, creates
 * {@link EnterpriseSerialJUnitClassRunner} otherwise.
 */
public class EnterpriseParametersRunnerFactory extends HazelcastParametersRunnerFactory {

    @Override
    protected HazelcastSerialClassRunner getSerialClassRunner(Class<?> testClass, Object[] parameters, String testName)
            throws InitializationError {
        GroupProperty.ENTERPRISE_LICENSE_KEY.setSystemProperty(UNLIMITED_LICENSE);
        return new EnterpriseSerialJUnitClassRunner(testClass, parameters, testName);
    }

    @Override
    protected HazelcastParallelClassRunner getParallelClassRunner(Class<?> testClass, Object[] parameters,
                                                                  String testName) throws InitializationError {
        GroupProperty.ENTERPRISE_LICENSE_KEY.setSystemProperty(UNLIMITED_LICENSE);
        return new EnterpriseParallelJUnitClassRunner(testClass, parameters, testName);
    }
}
