package com.hazelcast.enterprise;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.EnterpriseNodeExtension;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.EnterprisePhoneHome;
import com.hazelcast.internal.util.MD5Util;
import com.hazelcast.internal.util.PhoneHome;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

import static com.hazelcast.enterprise.SampleLicense.V5_ENTERPRISE_HD_SEC_40NODES_2080EXP;
import static com.hazelcast.enterprise.SampleLicense.V5_ENTERPRISE_HD_SEC_40NODES_2099EXP;
import static com.hazelcast.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EnterprisePhoneHomeTest extends HazelcastTestSupport {

    @Before
    public void before() {
        ClusterProperty.ENTERPRISE_LICENSE_KEY.setSystemProperty(V5_ENTERPRISE_HD_SEC_40NODES_2080EXP);
    }

    @Test
    public void testPhoneHomeEnterpriseParameters() {
        HazelcastInstance hz = createHazelcastInstance();
        Node node = getNode(hz);
        PhoneHome phoneHome = new EnterprisePhoneHome(node);
        checkPhoneHomeParameters(phoneHome, node);
    }

    @Test
    public void testPhoneHomeEnterpriseParameters_whenNewLicense() {
        HazelcastInstance hz = createHazelcastInstance();
        Node node = getNode(hz);
        PhoneHome phoneHome = new EnterprisePhoneHome(node);
        checkPhoneHomeParameters(phoneHome, node);
        EnterpriseNodeExtension enterpriseNodeExtension = (EnterpriseNodeExtension) node.getNodeExtension();
        enterpriseNodeExtension.setLicenseKey(V5_ENTERPRISE_HD_SEC_40NODES_2099EXP);
        checkPhoneHomeParameters(phoneHome, node);
    }

    private static void checkPhoneHomeParameters(PhoneHome phoneHome, Node node) {
        Map<String, String> parameters = phoneHome.phoneHome(node, true);
        assertEquals(parameters.get("e"), "true");
        assertEquals(parameters.get("oem"), "false");
        assertEquals(parameters.get("l"), MD5Util.toMD5String(node.config.getLicenseKey()));
        assertEquals(parameters.get("hdgb"), "0");
    }
}
