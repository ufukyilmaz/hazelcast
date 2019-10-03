package com.hazelcast.internal.monitor.jmx;

import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.instance.impl.EnterpriseNodeExtension;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.internal.jmx.MBeanDataHolder;
import com.hazelcast.license.domain.License;
import com.hazelcast.license.exception.InvalidLicenseException;
import com.hazelcast.license.util.LicenseHelper;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.enterprise.SampleLicense.V5_ENTERPRISE_HD_SEC_10NODES_2099EXP;
import static com.hazelcast.enterprise.SampleLicense.V5_ENTERPRISE_HD_SEC_40NODES_2080EXP;
import static com.hazelcast.enterprise.SampleLicense.V5_ENTERPRISE_HD_SEC_40NODES_2099EXP;
import static com.hazelcast.enterprise.SampleLicense.V5_ENTERPRISE_HD_SEC_CF_RU_40NODES_2099EXP;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class LicenseInfoMBeanTest extends HazelcastTestSupport {

    private static final String TYPE_NAME = "HazelcastInstance.LicenseInfo";

    private TestHazelcastInstanceFactory hazelcastInstanceFactory = createHazelcastInstanceFactory(1);
    private MBeanDataHolder holder;
    private String objectName;

    @Before
    public void setUp() {
        GroupProperty.ENTERPRISE_LICENSE_KEY.setSystemProperty(V5_ENTERPRISE_HD_SEC_40NODES_2080EXP);
        holder = new MBeanDataHolder(hazelcastInstanceFactory);
        objectName = "node" + holder.getHz().getCluster().getLocalMember().getAddress();
        holder.assertMBeanExistEventually(TYPE_NAME, objectName);
    }

    @After
    public void tearDown() {
        hazelcastInstanceFactory.shutdownAll();
    }

    @Test
    public void testInfo() throws Exception {
        HazelcastInstanceImpl instance = ((HazelcastInstanceProxy) holder.getHz()).getOriginal();
        License license = LicenseHelper.getLicense(instance.getConfig().getLicenseKey(),
                instance.node.getBuildInfo().getVersion());
        checkMBeanLicenseMatches(license);
    }

    @Test
    public void testMBeanNotified_whenLicenseUpdated() throws Exception {
        HazelcastInstanceImpl instance = ((HazelcastInstanceProxy) holder.getHz()).getOriginal();
        EnterpriseNodeExtension nodeExtension = (EnterpriseNodeExtension) instance.node.getNodeExtension();
        nodeExtension.setLicenseKey(V5_ENTERPRISE_HD_SEC_40NODES_2099EXP);
        License license = LicenseHelper.getLicense(instance.getConfig().getLicenseKey(),
                instance.node.getBuildInfo().getVersion());
        checkMBeanLicenseMatches(license);
    }

    @Test
    public void testMBeanNotNotified_whenLicenseInvalid() throws Exception {
        doTestLicenseInvalid("invalid");
    }

    @Test
    public void testMBeanNotNotified_whenLicenseIncompatibleFeatures() throws Exception {
        doTestLicenseInvalid(V5_ENTERPRISE_HD_SEC_CF_RU_40NODES_2099EXP);
    }

    @Test
    public void testMBeanNotNotified_whenLicenseDifferentAllowedNumberOfNodes() throws Exception {
        doTestLicenseInvalid(V5_ENTERPRISE_HD_SEC_10NODES_2099EXP);
    }

    @Test
    public void testMBeanNotNotified_whenLicenseOlderExpiryDate() throws Exception {
        // first change to a license with a longer expiry date
        HazelcastInstanceImpl instance = ((HazelcastInstanceProxy) holder.getHz()).getOriginal();
        EnterpriseNodeExtension nodeExtension = (EnterpriseNodeExtension) instance.node.getNodeExtension();
        nodeExtension.setLicenseKey(V5_ENTERPRISE_HD_SEC_40NODES_2099EXP);
        License license = LicenseHelper.getLicense(instance.getConfig().getLicenseKey(),
                instance.node.getBuildInfo().getVersion());
        checkMBeanLicenseMatches(license);
        // and then try to switch back to the original license
        doTestLicenseInvalid(V5_ENTERPRISE_HD_SEC_40NODES_2080EXP);
    }

    private void doTestLicenseInvalid(String licenseKey) throws Exception {
        HazelcastInstanceImpl instance = ((HazelcastInstanceProxy) holder.getHz()).getOriginal();
        License license = LicenseHelper.getLicense(instance.getConfig().getLicenseKey(),
                instance.node.getBuildInfo().getVersion());
        EnterpriseNodeExtension nodeExtension = (EnterpriseNodeExtension) instance.node.getNodeExtension();
        try {
            nodeExtension.setLicenseKey(licenseKey);
            fail("Exception expected");
        } catch (InvalidLicenseException e) {
            //expected
        }
        checkMBeanLicenseMatches(license);
    }

    private void checkMBeanLicenseMatches(License license) throws Exception {
        String maxNodeCountAllowed = getStringAttribute("maxNodeCountAllowed");
        String expiryDate = getStringAttribute("expiryDate");
        String typeCode = getStringAttribute("typeCode");
        String type = getStringAttribute("type");
        String ownerEmail = getStringAttribute("ownerEmail");
        String companyName = getStringAttribute("companyName");
        String keyHash = getStringAttribute("keyHash");

        assertEquals(license.getAllowedNumberOfNodes(), Integer.parseInt(maxNodeCountAllowed));
        assertEquals(MILLISECONDS.toDays(license.getExpiryDate().getTime()), MILLISECONDS.toDays(Long.parseLong(expiryDate)));
        if (license.getType() == null) {
            assertNull(typeCode);
            assertNull(type);
        } else {
            assertEquals(license.getType().getCode(), typeCode == null ? (Long) null : Integer.parseInt(typeCode));
            assertEquals(license.getType().getText(), type);
        }
        assertEquals(license.getEmail(), ownerEmail);
        assertEquals(license.getCompanyName(), companyName);
        assertEquals(license.computeKeyHash(), keyHash);
    }

    private String getStringAttribute(String name) throws Exception {
        return (String) holder.getMBeanAttribute(TYPE_NAME, objectName, name);
    }

}
