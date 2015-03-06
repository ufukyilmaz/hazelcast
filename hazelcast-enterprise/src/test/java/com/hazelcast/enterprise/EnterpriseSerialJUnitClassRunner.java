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
            System.setProperty(GroupProperties.PROP_ENTERPRISE_LICENSE_KEY, "Hazelcast Enterprise|9999 Nodes|9999 Clients|HD Memory: 99999999GB|37V1IyuEKczAfiwJk5HlSmb15999qC1992d999190L929109p9Z5C99ZgQ99");
        }
    }
}
