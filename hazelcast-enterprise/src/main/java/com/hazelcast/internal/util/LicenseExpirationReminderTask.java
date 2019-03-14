package com.hazelcast.internal.util;

import com.hazelcast.license.domain.License;
import com.hazelcast.license.util.LicenseHelper;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.TaskScheduler;

import java.util.Calendar;
import java.util.Date;

import static com.hazelcast.internal.util.LicenseExpirationReminderTask.NotificationPeriod.NONE;
import static com.hazelcast.internal.util.LicenseExpirationReminderTask.NotificationPeriod.of;
import static com.hazelcast.license.util.LicenseHelper.getExpiryDateWithGracePeriod;
import static com.hazelcast.util.Clock.currentTimeMillis;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Scheduled task responsible to send log-notifications regarding license info, regarding expiration.
 * <p>
 * Note. This task is not be started in NLC mode. See {@link com.hazelcast.instance.EnterpriseNodeExtension}.
 */
public class LicenseExpirationReminderTask implements Runnable {

    private static final int SECONDS_IN_MIN = 60;
    private static final int SECONDS_IN_HOUR = 60 * SECONDS_IN_MIN;
    private static final int SECONDS_IN_DAY = 24 * SECONDS_IN_HOUR;
    private static final int SCHEDULING_DELAY_DAYS_DUE_TO_EXPIRATION = 60;
    private static final String BANNER_TEMPLATE = "%n"
            + "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ WARNING @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@%n"
            + "HAZELCAST LICENSE :EXPIRY_TIME:"
            + ":GRACE_PERIOD:"
            + "Your license holder is :CUSTOMER_EMAIL:, you should have them contact%n"
            + "our license renewal department, urgently on :HAZELCAST_EMAIL:%n" + "or call us on :HAZELCAST_PHONE_NUMBER:%n%n"
            + "Please quote license id :LICENSE_ID:%n%n"
            + "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@";

    enum NotificationPeriod {
        ADVISORY(SECONDS_IN_DAY, 60 * SECONDS_IN_DAY),
        WARNING(SECONDS_IN_HOUR, 30 * SECONDS_IN_DAY),
        ALERT(SECONDS_IN_HOUR / 2, 7 * SECONDS_IN_DAY),
        GRACE_WARNING(SECONDS_IN_HOUR, 30 * SECONDS_IN_DAY, true),
        GRACE_ALERT(SECONDS_IN_HOUR / 2, 7 * SECONDS_IN_DAY, true),
        NONE(-1, -1);

        private final int notificationInterval;
        private final int limitInSecondsBeforePeriodAvails;
        private final boolean isGrace;

        NotificationPeriod(int notificationInterval, int limitInSecondsBeforePeriodAvails, boolean isGrace) {
            this.notificationInterval = notificationInterval;
            this.limitInSecondsBeforePeriodAvails = limitInSecondsBeforePeriodAvails;
            this.isGrace = isGrace;
        }

        NotificationPeriod(int notificationInterval, int limitInSecondsBeforePeriodAvails) {
            this(notificationInterval, limitInSecondsBeforePeriodAvails, false);
        }

        public int getNotificationInterval() {
            return notificationInterval;
        }

        static NotificationPeriod of(final long expiryDateInSeconds, final boolean isGrace) {
            if (expiryDateInSeconds <= ALERT.limitInSecondsBeforePeriodAvails) {
                return isGrace ? GRACE_ALERT : ALERT;
            } else if (expiryDateInSeconds <= WARNING.limitInSecondsBeforePeriodAvails) {
                return isGrace ? GRACE_WARNING : WARNING;
            } else if (expiryDateInSeconds <= ADVISORY.limitInSecondsBeforePeriodAvails) {
                return isGrace ? NONE : ADVISORY;
            }

            return NONE;
        }
    }

    private final License license;
    private final TaskScheduler scheduler;
    private final ILogger logger = Logger.getLogger(LicenseExpirationReminderTask.class);
    // only used for testing
    private final long nowInMillis;

    private LicenseExpirationReminderTask(TaskScheduler scheduler, License license) {
        this(scheduler, license, -1);
    }

    // this constructor is only used for testing purposes
    LicenseExpirationReminderTask(TaskScheduler scheduler, License license, long nowInMillis) {
        this.license = license;
        this.scheduler = scheduler;
        this.nowInMillis = nowInMillis;
    }

    @Override
    public void run() {
        final long nowInMillis = this.nowInMillis == -1 ? currentTimeMillis() : this.nowInMillis;

        NotificationPeriod period = NONE;
        try {
            period = calculateNotificationPeriod(nowInMillis);
            if (!NONE.equals(period)) {
                logger.warning(assembleLicenseInfoBanner(period, nowInMillis));
            }
        } finally {
            long rescheduleDelay = calcSchedulingDelay(period, nowInMillis);
            scheduler.schedule(this, rescheduleDelay, SECONDS);
            logger.fine("Current period " + period + ". Rescheduling check, in " + rescheduleDelay + " seconds. "
                    + "Expiration " + license.getExpiryDate());
        }
    }

    public static void scheduleWith(final TaskScheduler scheduler, final License license) {
        LicenseExpirationReminderTask task = new LicenseExpirationReminderTask(scheduler, license);
        scheduler.schedule(task, 0, SECONDS);
    }

    NotificationPeriod calculateNotificationPeriod(long nowInMillis) {
        Date expiryDate = license.getExpiryDate();

        boolean isGrace = false;
        if (expiryDate.getTime() <= nowInMillis
                && license.getGracePeriod() > 0) {
            expiryDate = getExpiryDateWithGracePeriod(license);
            isGrace = true;
        }

        return of(MILLISECONDS.toSeconds(expiryDate.getTime() - nowInMillis), isGrace);
    }

    String assembleLicenseInfoBanner(NotificationPeriod period, long nowInMillis) {
        return format(BANNER_TEMPLATE).replace(":EXPIRY_TIME:", formatExpiryTimeString(nowInMillis))
                .replace(":CUSTOMER_EMAIL:", license.getEmail() != null ? license.getEmail() : "not-specified")
                .replace(":HAZELCAST_EMAIL:", "info@hazelcast.com")
                .replace(":HAZELCAST_PHONE_NUMBER:", "+1 (650) 521-5453")
                .replace(":LICENSE_ID:", license.getKey())
                .replace(":GRACE_PERIOD:", period.isGrace && remainingDaysTillGraceEnds(nowInMillis) != -1
                        ? "You are now in a grace period of " + license.getGracePeriod() + " month(s). "
                        + format("The license will expire in " + remainingDaysTillGraceEnds(nowInMillis) + " days time%n%n")
                        : "");
    }

    long calcSchedulingDelay(NotificationPeriod period, long nowInMillis) {
        if (!NONE.equals(period)) {
            return period.notificationInterval;
        }

        Calendar scheduledDate = Calendar.getInstance();
        scheduledDate.setTime(license.getExpiryDate());
        scheduledDate.add(Calendar.DATE, -SCHEDULING_DELAY_DAYS_DUE_TO_EXPIRATION);
        if (scheduledDate.getTimeInMillis() <= nowInMillis) {
            throw new IllegalStateException("Notification period not NONE & scheduledDate in the past");
        }

        return MILLISECONDS.toSeconds(scheduledDate.getTimeInMillis() - nowInMillis);
    }

    private int remainingDaysTillGraceEnds(long nowInMillis) {
        long diff = LicenseHelper.getExpiryDateWithGracePeriod(license).getTime() - nowInMillis;
        return diff > 0 ? (int) MILLISECONDS.toDays(diff) : -1;
    }

    private String formatExpiryTimeString(long nowInMillis) {
        long diff = license.getExpiryDate().getTime() - nowInMillis;
        if (diff <= 0) {
            return format("EXPIRED!%n%n");
        }

        long days = MILLISECONDS.toDays(diff);
        if (days > 0) {
            return format("WILL EXPIRE IN %d DAYS.%n"
                    + "Your Hazelcast cluster will stop working on next re-start after expiry.%n%n", days);
        }

        long hours = MILLISECONDS.toHours(diff);
        if (hours > 0) {
            return format("WILL EXPIRE IN %d HOURS.%n"
                    + "Your Hazelcast cluster will stop working on next re-start after expiry.%n%n", hours);
        }

        long minutes = MILLISECONDS.toMinutes(diff);
        if (minutes > 0) {
            return format("WILL EXPIRE IN %d MINS.%n"
                    + "Your Hazelcast cluster will stop working on next re-start after expiry.%n%n", minutes);
        }

        return format("WILL EXPIRE IN A FEW SECONDS.%n"
                + "Your Hazelcast cluster will stop working on next re-start after expiry.%n%n");
    }
}
