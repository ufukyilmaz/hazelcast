package com.hazelcast.internal.monitor.jmx;

import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.internal.jmx.MBeanDataHolder;
import com.hazelcast.license.domain.License;
import com.hazelcast.license.util.LicenseHelper;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class LicenseInfoMBeanTest
        extends HazelcastTestSupport {

    private static final String TYPE_NAME = "HazelcastInstance.LicenseInfo";

    private TestHazelcastInstanceFactory hazelcastInstanceFactory = createHazelcastInstanceFactory(1);
    private MBeanDataHolder holder = new MBeanDataHolder(hazelcastInstanceFactory);

    private String objectName;

    @Before
    public void setUp() {
        objectName = "node" + holder.getHz().getCluster().getLocalMember().getAddress();
        holder.assertMBeanExistEventually(TYPE_NAME, objectName);
    }

    @After
    public void tearDown() {
        hazelcastInstanceFactory.shutdownAll();
    }

    @Test
    public void testInfo() throws Exception {
        String maxNodeCountAllowed = getStringAttribute("maxNodeCountAllowed");
        String expiryDate = getStringAttribute("expiryDate");
        String typeCode = getStringAttribute("typeCode");
        String type = getStringAttribute("type");
        String ownerEmail = getStringAttribute("ownerEmail");
        String companyName = getStringAttribute("companyName");
        String keyHash = getStringAttribute("keyHash");

        HazelcastInstanceImpl instance = ((HazelcastInstanceProxy) holder.getHz()).getOriginal();
        License license = LicenseHelper.getLicense(instance.getConfig().getLicenseKey(),
                instance.node.getBuildInfo().getVersion());

        assertEquals(license.getAllowedNumberOfNodes(), Integer.parseInt(maxNodeCountAllowed));
        assertEquals(MILLISECONDS.toDays(license.getExpiryDate().getTime()), MILLISECONDS.toDays(Long.parseLong(expiryDate)));
        assertEquals(license.getType().getCode(), Integer.parseInt(typeCode));
        assertEquals(license.getType().getText(), type);
        assertEquals(license.getEmail(), ownerEmail);
        assertEquals(license.getCompanyName(), companyName);
        assertEquals(license.computeKeyHash(), keyHash);
    }

    private String getStringAttribute(String name) throws Exception {
        return (String) holder.getMBeanAttribute(TYPE_NAME, objectName, name);
    }

}
