package com.hazelcast.enterprise;

import com.hazelcast.impl.GroupProperties;
import com.hazelcast.util.RandomBlockJUnit4ClassRunner;
import org.junit.runners.model.InitializationError;

/**
 * @mdogan 7/20/12
 */
public class EnterpriseJUnitClassRunner extends RandomBlockJUnit4ClassRunner {

    static {
        System.setProperty(GroupProperties.PROP_ENTERPRISE_LICENSE_KEY, "OFCDJMNIB9L121Q031S1S601U01206");
    }

    public EnterpriseJUnitClassRunner(Class<?> klass) throws InitializationError {
        super(klass);
    }
}
