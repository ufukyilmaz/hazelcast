package com.hazelcast.enterprise;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.EnterpriseNodeExtension;
import com.hazelcast.instance.Node;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.MD5Util;
import com.hazelcast.util.PhoneHome;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

import static com.hazelcast.enterprise.SampleLicense.V5_ENTERPRISE_HD_SEC_40NODES_2080EXP;
import static com.hazelcast.enterprise.SampleLicense.V5_ENTERPRISE_HD_SEC_40NODES_2099EXP;
import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class EnterprisePhoneHomeTest extends HazelcastTestSupport {

    @Before
    public void before() {
        GroupProperty.ENTERPRISE_LICENSE_KEY.setSystemProperty(V5_ENTERPRISE_HD_SEC_40NODES_2080EXP);
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
        Map<String, String> parameters = phoneHome.phoneHome(node);
        assertEquals(parameters.get("e"), "true");
        assertEquals(parameters.get("oem"), "false");
        assertEquals(parameters.get("l"), MD5Util.toMD5String(node.config.getLicenseKey()));
        assertEquals(parameters.get("hdgb"), "0");
    }
}
