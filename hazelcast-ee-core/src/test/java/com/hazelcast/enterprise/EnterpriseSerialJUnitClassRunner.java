package com.hazelcast.enterprise;

import com.hazelcast.instance.GroupProperties;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.runners.model.InitializationError;

/**
 * @mdogan 7/20/12
 */
public class EnterpriseSerialJUnitClassRunner extends HazelcastSerialClassRunner {

    static {
        if (System.getProperty(GroupProperties.PROP_ENTERPRISE_LICENSE_KEY) == null) {
            System.setProperty(GroupProperties.PROP_ENTERPRISE_LICENSE_KEY, "ENPDHM9KIAB45600S318R01S0W5162");
        }
    }

    public EnterpriseSerialJUnitClassRunner(Class<?> klass) throws InitializationError {
        super(klass);
    }
}
