package com.hazelcast.internal.ascii;

import com.hazelcast.spi.properties.ClusterProperty;
import org.junit.Before;

import static com.hazelcast.enterprise.SampleLicense.V5_ENTERPRISE_HD_SEC_40NODES_2080EXP;

public class RestLogLevelEnterpriseTest extends RestLogLevelTest {

    // behavior copies the community edition when Hazelcast security is not enabled

    @Before
    public void before() {
        ClusterProperty.ENTERPRISE_LICENSE_KEY.setSystemProperty(V5_ENTERPRISE_HD_SEC_40NODES_2080EXP);
    }

}
