package com.hazelcast.internal.util;

import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.license.domain.License;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Calendar;
import java.util.Date;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static junit.framework.TestCase.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category(QuickTest.class)
public class LicenseExpirationReminderTaskTest
        extends HazelcastTestSupport {

    private static final int ADVISORY_PERIOD_IN_DAYS = 59;
    private static final int WARNING_PERIOD_IN_DAYS = 29;
    private static final int ALERT_PERIOD_IN_DAYS = 6;

    @Test
    public void testAdvisoryPeriod() {
        License license = mock(License.class);
        when(license.getExpiryDate()).thenReturn(dateWithDaysDiff(ADVISORY_PERIOD_IN_DAYS));

        LicenseExpirationReminderTask task = new LicenseExpirationReminderTask(null, license);
        assertEquals(LicenseExpirationReminderTask.NotificationPeriod.ADVISORY, task.calculateNotificationPeriod());
    }

    @Test
    public void testWarningPeriod() {
        License license = mock(License.class);
        when(license.getExpiryDate()).thenReturn(dateWithDaysDiff(WARNING_PERIOD_IN_DAYS));

        LicenseExpirationReminderTask task = new LicenseExpirationReminderTask(null, license);
        assertEquals(LicenseExpirationReminderTask.NotificationPeriod.WARNING, task.calculateNotificationPeriod());
    }

    @Test
    public void testAlertPeriod() {
        License license = mock(License.class);
        when(license.getExpiryDate()).thenReturn(dateWithDaysDiff(ALERT_PERIOD_IN_DAYS));

        LicenseExpirationReminderTask task = new LicenseExpirationReminderTask(null, license);
        assertEquals(LicenseExpirationReminderTask.NotificationPeriod.ALERT, task.calculateNotificationPeriod());
    }

    @Test
    public void testAlertPeriod_whenExpired_noGrace() {
        License license = mock(License.class);
        when(license.getExpiryDate()).thenReturn(dateWithDaysDiff(-1));

        LicenseExpirationReminderTask task = new LicenseExpirationReminderTask(null, license);
        assertEquals(LicenseExpirationReminderTask.NotificationPeriod.ALERT, task.calculateNotificationPeriod());
    }

    @Test
    public void testWarningPeriod_whenExpired_withOneMonthGrace() {
        License license = mock(License.class);
        when(license.getExpiryDate()).thenReturn(dateWithDaysDiff(-1));
        when(license.getGracePeriod()).thenReturn(1);

        LicenseExpirationReminderTask task = new LicenseExpirationReminderTask(null, license);
        assertEquals(LicenseExpirationReminderTask.NotificationPeriod.GRACE_WARNING, task.calculateNotificationPeriod());
    }

    @Test
    public void testAlertPeriod_whenExpired_withOneWeekGrace() {
        License license = mock(License.class);
        when(license.getExpiryDate()).thenReturn(dateWithDaysDiff(-25));
        when(license.getGracePeriod()).thenReturn(1);

        LicenseExpirationReminderTask task = new LicenseExpirationReminderTask(null, license);
        assertEquals(LicenseExpirationReminderTask.NotificationPeriod.GRACE_ALERT, task.calculateNotificationPeriod());
    }

    @Test
    public void testAlertPeriod_whenExpired_withExpiredGrace() {
        License license = mock(License.class);
        when(license.getExpiryDate()).thenReturn(dateWithDaysDiff(-31));
        when(license.getGracePeriod()).thenReturn(1);

        LicenseExpirationReminderTask task = new LicenseExpirationReminderTask(null, license);
        assertEquals(LicenseExpirationReminderTask.NotificationPeriod.GRACE_ALERT, task.calculateNotificationPeriod());
    }

    @Test
    public void testValidPeriod() {
        License license = mock(License.class);
        when(license.getExpiryDate()).thenReturn(dateWithDaysDiff(61));

        LicenseExpirationReminderTask task = new LicenseExpirationReminderTask(null, license);
        assertEquals(LicenseExpirationReminderTask.NotificationPeriod.NONE, task.calculateNotificationPeriod());
    }

    @Test
    public void testLicenseInfoBanner() {
        License license = mock(License.class);
        when(license.getKey()).thenReturn("CUSTOM_TEST_KEY");
        when(license.getExpiryDate()).thenReturn(dateWithDaysDiff(ALERT_PERIOD_IN_DAYS));
        when(license.getEmail()).thenReturn("customer@example-company.com");

        LicenseExpirationReminderTask task = new LicenseExpirationReminderTask(null, license);
        String actual = task.assembleLicenseInfoBanner(LicenseExpirationReminderTask.NotificationPeriod.NONE);

        String expected = format("%n"
                + "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ WARNING @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@%n"
                + "HAZELCAST LICENSE WILL EXPIRE IN 6 DAYS.%n" + "Your Hazelcast cluster will stop working on next re-start after expiry.%n"
                + "%n" + "Your license holder is customer@example-company.com, you should have them contact%n"
                + "our license renewal department, urgently on info@hazelcast.com%n" + "or call us on +1 (650) 521-5453%n%n"
                + "Please quote license id CUSTOM_TEST_KEY%n%n"
                + "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");

        assertEquals(expected, actual);
    }

    @Test
    public void testLicenseInfoBanner_whenGracePeriod() {
        License license = mock(License.class);
        when(license.getKey()).thenReturn("CUSTOM_TEST_KEY");
        when(license.getExpiryDate()).thenReturn(dateWithDaysDiff(-1));
        when(license.getGracePeriod()).thenReturn(1);
        when(license.getEmail()).thenReturn("customer@example-company.com");

        LicenseExpirationReminderTask task = new LicenseExpirationReminderTask(null, license);
        task.calculateNotificationPeriod();

        String actual = task.assembleLicenseInfoBanner(LicenseExpirationReminderTask.NotificationPeriod.GRACE_WARNING);

        String expected = format("%n"
                + "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ WARNING @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@%n"
                + "HAZELCAST LICENSE EXPIRED!%n%n"
                + "You are now in a grace period of 1 month(s). The license will expire in "
                + countDaysTillSameDayNextMonth(license.getExpiryDate()) + " days time%n%n"
                + "Your license holder is customer@example-company.com, you should have them contact%n"
                + "our license renewal department, urgently on info@hazelcast.com%n" + "or call us on +1 (650) 521-5453%n%n"
                + "Please quote license id CUSTOM_TEST_KEY%n%n"
                + "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");

        assertEquals(expected, actual);
    }

    private int countDaysTillSameDayNextMonth(Date expiryDate) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(expiryDate);
        cal.add(Calendar.MONTH, 1);
        return (int) MILLISECONDS.toDays(cal.getTimeInMillis() - currentTimeMillis());
    }

    @Test
    public void testLicenseInfoBanner_whenGracePeriodExpired() {
        License license = mock(License.class);
        when(license.getKey()).thenReturn("CUSTOM_TEST_KEY");
        when(license.getExpiryDate()).thenReturn(dateWithDaysDiff(-31));
        when(license.getGracePeriod()).thenReturn(1);
        when(license.getEmail()).thenReturn("customer@example-company.com");

        LicenseExpirationReminderTask task = new LicenseExpirationReminderTask(null, license);
        task.calculateNotificationPeriod();

        String actual = task.assembleLicenseInfoBanner(LicenseExpirationReminderTask.NotificationPeriod.GRACE_ALERT);

        String expected = format("%n"
                + "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ WARNING @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@%n"
                + "HAZELCAST LICENSE EXPIRED!%n%n"
                + "Your license holder is customer@example-company.com, you should have them contact%n"
                + "our license renewal department, urgently on info@hazelcast.com%n" + "or call us on +1 (650) 521-5453%n%n"
                + "Please quote license id CUSTOM_TEST_KEY%n%n"
                + "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");


        assertEquals(expected, actual);
    }

    @Test
    public void testSchedulingDelay_expired() {
        License license = mock(License.class);
        when(license.getExpiryDate()).thenReturn(dateWithDaysDiff(-1));

        LicenseExpirationReminderTask task = new LicenseExpirationReminderTask(null, license);
        LicenseExpirationReminderTask.NotificationPeriod period = task.calculateNotificationPeriod();

        long scheduleDelay = task.calcSchedulingDelay(period);
        assertEquals(period.getNotificationInterval(), scheduleDelay);
    }

    @Test
    public void testSchedulingDelay_expiresInAMonth() {
        License license = mock(License.class);
        when(license.getExpiryDate()).thenReturn(dateWithDaysDiff(31));

        LicenseExpirationReminderTask task = new LicenseExpirationReminderTask(null, license);
        LicenseExpirationReminderTask.NotificationPeriod period = task.calculateNotificationPeriod();

        long scheduleDelay = task.calcSchedulingDelay(period);
        assertEquals(period.getNotificationInterval(), scheduleDelay);
    }

    @Test
    public void testSchedulingDelay_expiresInTwoMonths() {
        License license = mock(License.class);
        when(license.getExpiryDate()).thenReturn(dateWithDaysDiff(61));

        LicenseExpirationReminderTask task = new LicenseExpirationReminderTask(null, license);
        LicenseExpirationReminderTask.NotificationPeriod period = task.calculateNotificationPeriod();

        long scheduleDelay = task.calcSchedulingDelay(period);
        assertEquals(1, SECONDS.toDays(scheduleDelay));
    }

    @Test
    public void testSchedulingDelay_expiresInThreeMonths() {
        License license = mock(License.class);
        when(license.getExpiryDate()).thenReturn(dateWithDaysDiff(90));

        LicenseExpirationReminderTask task = new LicenseExpirationReminderTask(null, license);
        LicenseExpirationReminderTask.NotificationPeriod period = task.calculateNotificationPeriod();

        long scheduleDelay = task.calcSchedulingDelay(period);
        assertEquals(expectedSchedDelayDays(90), SECONDS.toDays(scheduleDelay));
    }

    @Test
    public void testSchedulingDelay_expiresInAYear() {
        License license = mock(License.class);
        when(license.getExpiryDate()).thenReturn(dateWithDaysDiff(365));

        LicenseExpirationReminderTask task = new LicenseExpirationReminderTask(null, license);
        LicenseExpirationReminderTask.NotificationPeriod period = task.calculateNotificationPeriod();

        long scheduleDelay = task.calcSchedulingDelay(period);
        // Should assert to expiration - 2 months

        assertEquals(expectedSchedDelayDays(365), SECONDS.toDays(scheduleDelay));
    }

    private Date dateWithDaysDiff(int diff) {
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE, diff);
        // Prevents testing from failing due to ms differences in time comparisons
        calendar.add(Calendar.HOUR, diff > 0 ? 1 : -1);
        return calendar.getTime();
    }

    private int expectedSchedDelayDays(int expirationDays) {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, expirationDays);
        cal.add(Calendar.DATE, -2 * 30);
        cal.add(Calendar.HOUR, 1);
        return (int) MILLISECONDS.toDays(cal.getTimeInMillis() - currentTimeMillis());
    }
}
