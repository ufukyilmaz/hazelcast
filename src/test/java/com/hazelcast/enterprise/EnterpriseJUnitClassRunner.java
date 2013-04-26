package com.hazelcast.enterprise;

import com.hazelcast.impl.GroupProperties;
import com.hazelcast.util.RandomBlockJUnit4ClassRunner;
import org.junit.runners.model.InitializationError;

/**
 * @mdogan 7/20/12
 */
public class EnterpriseJUnitClassRunner extends RandomBlockJUnit4ClassRunner {

    static {
        System.setProperty(GroupProperties.PROP_ENTERPRISE_LICENSE_KEY, "HIEMN9DJPCG824Y8142U0010Q82060");
    }

    public EnterpriseJUnitClassRunner(Class<?> klass) throws InitializationError {
        super(klass);
    }
}
