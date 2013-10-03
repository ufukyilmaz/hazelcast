package com.hazelcast.enterprise;

import com.hazelcast.instance.GroupProperties;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import org.junit.runners.model.InitializationError;

/**
 * @mdogan 7/20/12
 */
public class EnterpriseJUnitClassRunner extends HazelcastJUnit4ClassRunner {

    static {
        System.setProperty(GroupProperties.PROP_ENTERPRISE_LICENSE_KEY, "HIEMN9DJPCG824Y8142U0010Q82060");
    }

    public EnterpriseJUnitClassRunner(Class<?> klass) throws InitializationError {
        super(klass);
    }
}
