package com.hazelcast.enterprise;

import com.hazelcast.instance.GroupProperties;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.runners.model.InitializationError;

/**
 * @mdogan 7/20/12
 */
public class EnterpriseSerialJUnitClassRunner extends HazelcastSerialClassRunner {

    public EnterpriseSerialJUnitClassRunner(Class<?> klass) throws InitializationError {
        super(klass);
        if (System.getProperty(GroupProperties.PROP_ENTERPRISE_LICENSE_KEY) == null) {
            System.setProperty(GroupProperties.PROP_ENTERPRISE_LICENSE_KEY, "HazelcastEnterprise#9999Nodes#9999Clients#HDMemory:99999999GB#w7yAkRj1IbHcBfVimEOuKr638599939999peZ319999z05999Wn149zGxG09");
        }
    }
}
