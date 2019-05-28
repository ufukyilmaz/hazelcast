package com.hazelcast.enterprise;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.MD5Util;
import com.hazelcast.util.PhoneHome;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EnterprisePhoneHomeTest extends HazelcastTestSupport {

    @Test
    public void testPhoneHomeEnterpriseParameters() {
        HazelcastInstance hz = createHazelcastInstance();
        Node node = getNode(hz);
        PhoneHome phoneHome = new EnterprisePhoneHome(node);
        String licenseKey = node.getConfig().getLicenseKey();

        Map<String, String> parameters = phoneHome.phoneHome(node, true);
        assertEquals(parameters.get("e"), "true");
        assertEquals(parameters.get("oem"), "false");
        assertEquals(parameters.get("l"), MD5Util.toMD5String(licenseKey));
        assertEquals(parameters.get("hdgb"), "0");
    }
}
