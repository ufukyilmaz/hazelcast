package com.hazelcast.internal.util;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.instance.impl.EnterpriseNodeExtension;
import com.hazelcast.license.domain.License;
import com.hazelcast.license.exception.InvalidLicenseException;
import com.hazelcast.spi.impl.executionservice.TaskScheduler;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;

import java.util.Calendar;
import java.util.Date;

import static com.hazelcast.enterprise.SampleLicense.V5_ENTERPRISE_HD_SEC_10NODES_2099EXP;
import static com.hazelcast.enterprise.SampleLicense.V5_ENTERPRISE_HD_SEC_40NODES_2080EXP;
import static com.hazelcast.enterprise.SampleLicense.V5_ENTERPRISE_HD_SEC_40NODES_2099EXP;
import static com.hazelcast.enterprise.SampleLicense.V5_ENTERPRISE_HD_SEC_CF_RU_40NODES_2099EXP;
import static com.hazelcast.internal.util.Clock.currentTimeMillis;
import static com.hazelcast.internal.util.LicenseExpirationReminderTask.NotificationPeriod.ADVISORY;
import static com.hazelcast.internal.util.LicenseExpirationReminderTask.NotificationPeriod.ALERT;
import static com.hazelcast.internal.util.LicenseExpirationReminderTask.NotificationPeriod.GRACE_ALERT;
import static com.hazelcast.internal.util.LicenseExpirationReminderTask.NotificationPeriod.GRACE_WARNING;
import static com.hazelcast.internal.util.LicenseExpirationReminderTask.NotificationPeriod.NONE;
import static com.hazelcast.internal.util.LicenseExpirationReminderTask.NotificationPeriod.WARNING;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(EnterpriseSerialJUnitClassRunner.class)
@PrepareForTest(LicenseExpirationReminderTask.Factory.class)
@PowerMockIgnore({"javax.*", "com.sun.*", "org.apache.logging.log4j.*"})
@Category({QuickTest.class, ParallelJVMTest.class})
public class LicenseExpirationReminderTaskTest extends HazelcastTestSupport {

    private static final int ADVISORY_PERIOD_IN_DAYS = 59;
    private static final int WARNING_PERIOD_IN_DAYS = 29;
    private static final int ALERT_PERIOD_IN_DAYS = 6;

    private static String msg(long nowInMillis) {
        return format("failed for nowInMillis: %d", nowInMillis);
    }

    @Before
    public void before() {
        GroupProperty.ENTERPRISE_LICENSE_KEY.setSystemProperty(V5_ENTERPRISE_HD_SEC_40NODES_2080EXP);
    }

    @Test
    public void testAdvisoryPeriod() {
        final long nowInMillis = currentTimeMillis();

        License license = mock(License.class);
        when(license.getExpiryDate()).thenReturn(dateWithDaysDiff(ADVISORY_PERIOD_IN_DAYS, nowInMillis));

        LicenseExpirationReminderTask task = new LicenseExpirationReminderTask(null, license, nowInMillis);
        assertEquals(msg(nowInMillis), ADVISORY, task.calculateNotificationPeriod(nowInMillis));
    }

    @Test
    public void testWarningPeriod() {
        final long nowInMillis = currentTimeMillis();

        License license = mock(License.class);
        when(license.getExpiryDate()).thenReturn(dateWithDaysDiff(WARNING_PERIOD_IN_DAYS, nowInMillis));

        LicenseExpirationReminderTask task = new LicenseExpirationReminderTask(null, license, nowInMillis);
        assertEquals(msg(nowInMillis), WARNING, task.calculateNotificationPeriod(nowInMillis));
    }

    @Test
    public void testAlertPeriod() {
        final long nowInMillis = currentTimeMillis();

        License license = mock(License.class);
        when(license.getExpiryDate()).thenReturn(dateWithDaysDiff(ALERT_PERIOD_IN_DAYS, nowInMillis));

        LicenseExpirationReminderTask task = new LicenseExpirationReminderTask(null, license, nowInMillis);
        assertEquals(msg(nowInMillis), ALERT, task.calculateNotificationPeriod(nowInMillis));
    }

    @Test
    public void testAlertPeriod_whenExpired_noGrace() {
        final long nowInMillis = currentTimeMillis();

        License license = mock(License.class);
        when(license.getExpiryDate()).thenReturn(dateWithDaysDiff(-1, nowInMillis));

        LicenseExpirationReminderTask task = new LicenseExpirationReminderTask(null, license, nowInMillis);
        assertEquals(msg(nowInMillis), ALERT, task.calculateNotificationPeriod(nowInMillis));
    }

    @Test
    public void testWarningPeriod_whenExpired_withOneMonthGrace() {
        final long nowInMillis = currentTimeMillis();

        License license = mock(License.class);
        when(license.getExpiryDate()).thenReturn(dateWithDaysDiff(-1, nowInMillis));
        when(license.getGracePeriod()).thenReturn(1);

        LicenseExpirationReminderTask task = new LicenseExpirationReminderTask(null, license, nowInMillis);
        assertEquals(msg(nowInMillis), GRACE_WARNING, task.calculateNotificationPeriod(nowInMillis));
    }

    @Test
    public void testAlertPeriod_whenExpired_withOneWeekGrace() {
        final long nowInMillis = currentTimeMillis();

        License license = mock(License.class);
        when(license.getExpiryDate()).thenReturn(dateWithDaysDiff(-25, nowInMillis));
        when(license.getGracePeriod()).thenReturn(1);

        LicenseExpirationReminderTask task = new LicenseExpirationReminderTask(null, license, nowInMillis);
        assertEquals(msg(nowInMillis), GRACE_ALERT, task.calculateNotificationPeriod(nowInMillis));
    }

    @Test
    public void testAlertPeriod_whenExpired_withExpiredGrace() {
        final long nowInMillis = currentTimeMillis();

        License license = mock(License.class);
        when(license.getExpiryDate()).thenReturn(dateWithDaysDiff(-31, nowInMillis));
        when(license.getGracePeriod()).thenReturn(1);

        LicenseExpirationReminderTask task = new LicenseExpirationReminderTask(null, license, nowInMillis);
        assertEquals(msg(nowInMillis), GRACE_ALERT, task.calculateNotificationPeriod(nowInMillis));
    }

    @Test
    public void testValidPeriod() {
        final long nowInMillis = currentTimeMillis();

        License license = mock(License.class);
        when(license.getExpiryDate()).thenReturn(dateWithDaysDiff(61, nowInMillis));

        LicenseExpirationReminderTask task = new LicenseExpirationReminderTask(null, license, nowInMillis);
        assertEquals(msg(nowInMillis), NONE, task.calculateNotificationPeriod(nowInMillis));
    }

    @Test
    public void testLicenseInfoBanner() {
        final long nowInMillis = currentTimeMillis();

        License license = mock(License.class);
        when(license.getKey()).thenReturn("CUSTOM_TEST_KEY");
        when(license.getExpiryDate()).thenReturn(dateWithDaysDiff(ALERT_PERIOD_IN_DAYS, nowInMillis));
        when(license.getEmail()).thenReturn("customer@example-company.com");

        LicenseExpirationReminderTask task = new LicenseExpirationReminderTask(null, license, nowInMillis);
        String actual = task.assembleLicenseInfoBanner(NONE, nowInMillis);

        String expected = format("%n"
                + "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ WARNING @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@%n"
                + "THIS HAZELCAST LICENSE ID CUSTOM_TEST_KEY WILL EXPIRE IN 6 DAYS.%n"
                + "Your Hazelcast cluster will stop working on the next re-start after%n"
                + "expiry.%n%n"
                + "Please contact your Hazelcast Account Executive or%n"
                + "email sales@hazelcast.com.%n"
                + "Phone: +1 (650) 521-5453%n"
                + "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");

        assertEquals(msg(nowInMillis), expected, actual);
    }

    @Test
    public void testLicenseInfoBanner_whenGracePeriod() {
        final long nowInMillis = currentTimeMillis();

        License license = mock(License.class);
        when(license.getKey()).thenReturn("CUSTOM_TEST_KEY");
        when(license.getExpiryDate()).thenReturn(dateWithDaysDiff(-1, nowInMillis));
        when(license.getGracePeriod()).thenReturn(1);
        when(license.getEmail()).thenReturn("customer@example-company.com");

        LicenseExpirationReminderTask task = new LicenseExpirationReminderTask(null, license, nowInMillis);
        task.calculateNotificationPeriod(nowInMillis);

        String actual = task.assembleLicenseInfoBanner(GRACE_WARNING, nowInMillis);

        String expected = format("%n"
                + "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ WARNING @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@%n"
                + "THIS HAZELCAST LICENSE ID CUSTOM_TEST_KEY HAS EXPIRED!%n%n"
                + "You are now in a grace period of 1 month(s). The license will expire%n"
                + "in " + countDaysTillSameDayNextMonth(license.getExpiryDate(), nowInMillis) + " days time.%n%n"
                + "Please contact your Hazelcast Account Executive or%n"
                + "email sales@hazelcast.com.%n"
                + "Phone: +1 (650) 521-5453%n"
                + "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");

        assertEquals(msg(nowInMillis), expected, actual);
    }

    private int countDaysTillSameDayNextMonth(Date expiryDate, long nowInMillis) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(expiryDate);
        cal.add(Calendar.MONTH, 1);
        return (int) MILLISECONDS.toDays(cal.getTimeInMillis() - nowInMillis);
    }

    @Test
    public void testLicenseInfoBanner_whenGracePeriodExpired() {
        final long nowInMillis = currentTimeMillis();

        License license = mock(License.class);
        when(license.getKey()).thenReturn("CUSTOM_TEST_KEY");
        when(license.getExpiryDate()).thenReturn(dateWithDaysDiff(-31, nowInMillis));
        when(license.getGracePeriod()).thenReturn(1);
        when(license.getEmail()).thenReturn("customer@example-company.com");

        LicenseExpirationReminderTask task = new LicenseExpirationReminderTask(null, license, nowInMillis);
        task.calculateNotificationPeriod(nowInMillis);

        String actual = task.assembleLicenseInfoBanner(GRACE_ALERT, nowInMillis);

        String expected = format("%n"
                + "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ WARNING @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@%n"
                + "THIS HAZELCAST LICENSE ID CUSTOM_TEST_KEY HAS EXPIRED!%n%n"
                + "Please contact your Hazelcast Account Executive or%n"
                + "email sales@hazelcast.com.%n"
                + "Phone: +1 (650) 521-5453%n"
                + "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");


        assertEquals(msg(nowInMillis), expected, actual);
    }

    @Test
    public void testSchedulingDelay_expired() {
        final long nowInMillis = currentTimeMillis();

        License license = mock(License.class);
        when(license.getExpiryDate()).thenReturn(dateWithDaysDiff(-1, nowInMillis));

        LicenseExpirationReminderTask task = new LicenseExpirationReminderTask(null, license, nowInMillis);
        LicenseExpirationReminderTask.NotificationPeriod period = task.calculateNotificationPeriod(nowInMillis);

        long scheduleDelay = task.calcSchedulingDelay(period, nowInMillis);
        assertEquals(msg(nowInMillis), period.getNotificationInterval(), scheduleDelay);
    }

    @Test
    public void testSchedulingDelay_expiresInAMonth() {
        final long nowInMillis = currentTimeMillis();

        License license = mock(License.class);
        when(license.getExpiryDate()).thenReturn(dateWithDaysDiff(31, nowInMillis));

        LicenseExpirationReminderTask task = new LicenseExpirationReminderTask(null, license, nowInMillis);
        LicenseExpirationReminderTask.NotificationPeriod period = task.calculateNotificationPeriod(nowInMillis);

        long scheduleDelay = task.calcSchedulingDelay(period, nowInMillis);
        assertEquals(msg(nowInMillis), period.getNotificationInterval(), scheduleDelay);
    }

    @Test
    public void testSchedulingDelay_expiresInTwoMonths() {
        final long nowInMillis = currentTimeMillis();

        License license = mock(License.class);
        when(license.getExpiryDate()).thenReturn(dateWithDaysDiff(61, nowInMillis));

        LicenseExpirationReminderTask task = new LicenseExpirationReminderTask(null, license, nowInMillis);
        LicenseExpirationReminderTask.NotificationPeriod period = task.calculateNotificationPeriod(nowInMillis);

        long scheduleDelay = task.calcSchedulingDelay(period, nowInMillis);
        assertEquals(msg(nowInMillis), 1, SECONDS.toDays(scheduleDelay));
    }

    @Test
    public void testSchedulingDelay_expiresInThreeMonths() {
        final long nowInMillis = currentTimeMillis();

        License license = mock(License.class);
        when(license.getExpiryDate()).thenReturn(dateWithDaysDiff(90, nowInMillis));

        LicenseExpirationReminderTask task = new LicenseExpirationReminderTask(null, license, nowInMillis);
        LicenseExpirationReminderTask.NotificationPeriod period = task.calculateNotificationPeriod(nowInMillis);

        long scheduleDelay = task.calcSchedulingDelay(period, nowInMillis);
        assertEquals(msg(nowInMillis), expectedSchedDelayDays(90, nowInMillis), SECONDS.toDays(scheduleDelay));
    }

    @Test
    public void testSchedulingDelay_expiresInAYear() {
        final long nowInMillis = currentTimeMillis();

        License license = mock(License.class);
        when(license.getExpiryDate()).thenReturn(dateWithDaysDiff(365, nowInMillis));

        LicenseExpirationReminderTask task = new LicenseExpirationReminderTask(null, license, nowInMillis);
        LicenseExpirationReminderTask.NotificationPeriod period = task.calculateNotificationPeriod(nowInMillis);

        long scheduleDelay = task.calcSchedulingDelay(period, nowInMillis);
        // Should assert to expiration - 2 months

        assertEquals(msg(nowInMillis), expectedSchedDelayDays(365, nowInMillis),
                SECONDS.toDays(scheduleDelay));
    }

    private Date dateWithDaysDiff(int diff, long nowInMillis) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(nowInMillis);
        calendar.add(Calendar.DATE, diff);
        // Prevents testing from failing due to ms differences in time comparisons
        calendar.add(Calendar.HOUR, diff > 0 ? 1 : -1);
        return calendar.getTime();
    }

    private int expectedSchedDelayDays(int expirationDays, long nowInMillis) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(nowInMillis);
        cal.add(Calendar.DATE, expirationDays);
        cal.add(Calendar.DATE, -2 * 30);
        cal.add(Calendar.HOUR, 1);
        return (int) MILLISECONDS.toDays(cal.getTimeInMillis() - nowInMillis);
    }

    @Test
    public void testReminderTaskRescheduled_whenLicenseUpdated() throws Exception {
        LicenseExpirationReminderTask mock = mock(LicenseExpirationReminderTask.class);
        whenNew(LicenseExpirationReminderTask.class)
                .withArguments(any(TaskScheduler.class), any(License.class), any(Long.class)).thenReturn(mock);
        HazelcastInstance instance = createHazelcastInstance();
        setLicense(instance, V5_ENTERPRISE_HD_SEC_40NODES_2099EXP);
        ArgumentCaptor<License> captor = ArgumentCaptor.forClass(License.class);
        verify(mock).rescheduleWithNewLicense(captor.capture());
        License actual = captor.getValue();
        assertNotNull(actual);
        Calendar cal = Calendar.getInstance();
        cal.setTime(actual.getExpiryDate());
        assertEquals(2099, cal.get(Calendar.YEAR));
    }

    private void doTestReminderTaskInvalidLicense(String licenseKey) {
        LicenseExpirationReminderTask mock = mock(LicenseExpirationReminderTask.class);
        HazelcastInstance instance = createHazelcastInstance();
        try {
            setLicense(instance, licenseKey);
            fail("Exception expected");
        } catch (InvalidLicenseException e) {
            // expected
        }
        verify(mock, never()).rescheduleWithNewLicense(any(License.class));
    }

    @Test
    public void testReminderTaskNotRescheduled_whenLicenseInvalid() {
        doTestReminderTaskInvalidLicense("invalid");
    }

    @Test
    public void testReminderTaskNotRescheduled_whenLicenseIncompatibleFeatures() {
        doTestReminderTaskInvalidLicense(V5_ENTERPRISE_HD_SEC_CF_RU_40NODES_2099EXP);
    }

    @Test
    public void testReminderTaskNotRescheduled_whenLicenseDifferentAllowedNumberOfNodes() {
        doTestReminderTaskInvalidLicense(V5_ENTERPRISE_HD_SEC_10NODES_2099EXP);
    }

    @Test
    public void testReminderTaskNotRescheduled_whenLicenseOlderExpiryDate() {
        GroupProperty.ENTERPRISE_LICENSE_KEY.setSystemProperty(V5_ENTERPRISE_HD_SEC_40NODES_2099EXP);
        doTestReminderTaskInvalidLicense(V5_ENTERPRISE_HD_SEC_40NODES_2080EXP);
    }

    private static void setLicense(HazelcastInstance instance, String licenseKey) {
        EnterpriseNodeExtension enterpriseExtension = (EnterpriseNodeExtension) getNode(instance).getNodeExtension();
        enterpriseExtension.setLicenseKey(licenseKey);
    }

}
