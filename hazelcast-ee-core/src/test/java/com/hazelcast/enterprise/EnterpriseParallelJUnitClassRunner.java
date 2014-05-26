package com.hazelcast.enterprise;

import com.hazelcast.instance.GroupProperties;
import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.runners.model.InitializationError;

/**
 * @mdogan 7/20/12
 */
public class EnterpriseParallelJUnitClassRunner extends HazelcastParallelClassRunner {

    static {
        if (System.getProperty(GroupProperties.PROP_ENTERPRISE_LICENSE_KEY) == null) {
            System.setProperty(GroupProperties.PROP_ENTERPRISE_LICENSE_KEY, "ENPDHM9KIAB45600S318R01S0W5162");
        }
    }

    public EnterpriseParallelJUnitClassRunner(Class<?> klass) throws InitializationError {
        super(klass);
    }
}
