package com.hazelcast.enterprise;

import com.hazelcast.instance.GroupProperties;
import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.runners.model.InitializationError;

/**
 * @mdogan 7/20/12
 */
public class EnterpriseParallelJUnitClassRunner extends HazelcastParallelClassRunner {

    public EnterpriseParallelJUnitClassRunner(Class<?> klass) throws InitializationError {
        super(klass);
        if (System.getProperty(GroupProperties.PROP_ENTERPRISE_LICENSE_KEY) == null) {
            System.setProperty(GroupProperties.PROP_ENTERPRISE_LICENSE_KEY, SampleLicense.UNLIMITED_LICENSE);
        }
    }
}
