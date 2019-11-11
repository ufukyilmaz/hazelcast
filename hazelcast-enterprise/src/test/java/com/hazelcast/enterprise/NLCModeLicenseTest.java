package com.hazelcast.enterprise;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.EnterpriseNodeExtension;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.EnterprisePhoneHome;
import com.hazelcast.internal.util.LicenseExpirationReminderTask;
import com.hazelcast.internal.util.PhoneHome;
import com.hazelcast.license.domain.Feature;
import com.hazelcast.license.domain.License;
import com.hazelcast.license.nlc.BuiltInLicenseProviderFactory;
import com.hazelcast.license.nlc.impl.NLCLicenseProvider;
import com.hazelcast.license.util.LicenseHelper;
import com.hazelcast.spi.impl.executionservice.TaskScheduler;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.internal.verification.NoMoreInteractions;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import static com.hazelcast.enterprise.SampleLicense.EXPIRED_ENTERPRISE_LICENSE;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.spy;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

/**
 * Various tests for behavior in No License Checker (NLC) mode ("built-in license").
 */
@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(EnterpriseSerialJUnitClassRunner.class)
@PrepareForTest({LicenseHelper.class, BuiltInLicenseProviderFactory.class, LicenseExpirationReminderTask.class})
@PowerMockIgnore({"javax.*", "com.sun.*", "org.apache.logging.log4j.*"})
@Category(QuickTest.class)
public class NLCModeLicenseTest extends HazelcastTestSupport {

    @Test
    public void testNLCModeStartupWithNoLicense() throws Exception {
        turnOnNLCMode();

        Config config = new Config();
        config.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY.getName(), "");
        createHazelcastInstance(config);
    }

    @Test
    public void testNLCModeStartupIgnoreExpiredLicense() throws Exception {
        turnOnNLCMode();

        Config config = new Config();
        config.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY.getName(), EXPIRED_ENTERPRISE_LICENSE);
        createHazelcastInstance(config);
    }

    @Test
    public void testStartupSingleLicenseParse() {
        spy(LicenseHelper.class);

        createHazelcastInstance();

        verifyStatic(LicenseHelper.class);
        LicenseHelper.getLicense(any(String.class), any(String.class));
    }

    @Test
    public void testNLCModeStartupNoLicenseParse() throws Exception {
        turnOnNLCMode();
        spy(LicenseHelper.class);

        Config config = new Config();
        config.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY.getName(), "");
        createHazelcastInstance(config);

        verifyStatic(LicenseHelper.class);
        LicenseHelper.getBuiltInLicense();

        verifyStatic(LicenseHelper.class);
        LicenseHelper.isBuiltInLicense(any(License.class));

        verifyStatic(LicenseHelper.class);
        LicenseHelper.checkLicensePerFeature(any(License.class), any(Feature.class));

        // the most valuable check is here
        verifyStatic(LicenseHelper.class, new NoMoreInteractions());
        LicenseHelper.getLicense(any(String.class), any(String.class));
    }

    @Test
    public void testStartupScheduleReminderTask() {
        spy(LicenseExpirationReminderTask.class);

        createHazelcastInstance();

        verifyStatic(LicenseExpirationReminderTask.class);
        LicenseExpirationReminderTask.scheduleWith(any(TaskScheduler.class), any(License.class));
    }

    @Test
    public void testNLCModeStartupNoReminderTask() throws Exception {
        turnOnNLCMode();
        spy(LicenseExpirationReminderTask.class);

        Config config = new Config();
        config.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY.getName(), "");
        createHazelcastInstance(config);

        verifyStatic(LicenseExpirationReminderTask.class, new NoMoreInteractions());
        LicenseExpirationReminderTask.scheduleWith(any(TaskScheduler.class), any(License.class));
    }

    @Test
    public void testNLCModePhoneHomeEnterpriseParameters() throws Exception {
        turnOnNLCMode();

        HazelcastInstance hz = createHazelcastInstance();
        Node node = getNode(hz);
        PhoneHome phoneHome = new EnterprisePhoneHome(node);

        Map<String, String> parameters = phoneHome.phoneHome(node, true);
        assertEquals(parameters.get("e"), "true");
        assertEquals(parameters.get("oem"), "true");
        assertEquals(parameters.get("l"), "");
    }

    @Test
    public void testNLCModeEnablesAllFeatures() throws Exception {
        turnOnNLCMode();

        HazelcastInstance hz = createHazelcastInstance();
        Node node = getNode(hz);
        EnterpriseNodeExtension nodeExtension = (EnterpriseNodeExtension) node.getNodeExtension();
        License license = nodeExtension.getLicense();

        assertEquals(createAllV5FeaturesEnabledList(), license.getFeatures());
        assertEquals(license.getAllowedNumberOfNodes(), Integer.MAX_VALUE);

        Calendar cal = Calendar.getInstance();
        cal.setTime(license.getExpiryDate());
        int year = cal.get(Calendar.YEAR);
        assertEquals(2099, year);
    }

    /**
     * Emulates situation when NoLicenseProvider class is not available and thus cannot be loaded.
     */
    private void turnOnNLCMode() throws Exception {
        BuiltInLicenseProviderFactory providerFactoryMock = mock(BuiltInLicenseProviderFactory.class);
        whenNew(BuiltInLicenseProviderFactory.class).withNoArguments().thenReturn(providerFactoryMock);
        when(providerFactoryMock.create()).thenReturn(new NLCLicenseProvider());
    }

    private List<Feature> createAllV5FeaturesEnabledList() {
        List<Feature> allFeaturesEnableList = new ArrayList<Feature>();
        allFeaturesEnableList.add(Feature.MAN_CENTER);
        allFeaturesEnableList.add(Feature.CLUSTERED_JMX);
        allFeaturesEnableList.add(Feature.CLUSTERED_REST);
        allFeaturesEnableList.add(Feature.SECURITY);
        allFeaturesEnableList.add(Feature.WAN);
        allFeaturesEnableList.add(Feature.HD_MEMORY);
        allFeaturesEnableList.add(Feature.HOT_RESTART);
        allFeaturesEnableList.add(Feature.ROLLING_UPGRADE);
        allFeaturesEnableList.add(Feature.CLIENT_FILTERING);
        allFeaturesEnableList.add(Feature.CP_PERSISTENCE);
        return allFeaturesEnableList;
    }
}
