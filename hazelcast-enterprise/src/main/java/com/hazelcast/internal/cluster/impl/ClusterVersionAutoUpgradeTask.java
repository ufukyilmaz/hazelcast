package com.hazelcast.internal.cluster.impl;

import com.hazelcast.instance.impl.OutOfMemoryErrorDispatcher;
import com.hazelcast.logging.ILogger;
import com.hazelcast.version.Version;

import java.util.Random;

import static java.lang.String.format;

/**
 * This task tries to upgrade cluster version when auto upgrading is
 * enabled and all member versions in the cluster have been upgraded to
 * a version that is greater than current cluster version.
 *
 * Scheduling of this task can only be done on master node. In case of task
 * failure, it reschedules itself with a simple backoff strategy.
 */
class ClusterVersionAutoUpgradeTask implements Runnable {

    private static final int MIN_BACKOFF_DELAY_SECONDS = 1;
    private static final int MAX_BACKOFF_ATTEMPT_NUMBER = 5;

    private final int attemptNumber;
    private final Random random = new Random();
    private final ILogger logger;
    private final ClusterServiceImpl clusterService;
    private final ClusterVersionAutoUpgradeHelper autoUpgradeHelper;

    ClusterVersionAutoUpgradeTask(ClusterServiceImpl clusterService, int attemptNumber,
                                  ClusterVersionAutoUpgradeHelper autoUpgradeHelper) {
        this.attemptNumber = attemptNumber;
        this.clusterService = clusterService;
        this.autoUpgradeHelper = autoUpgradeHelper;
        this.logger = clusterService.getNodeEngine().getLogger(ClusterVersionAutoUpgradeTask.class);
    }

    @Override
    public void run() {
        MemberMap memberMap = autoUpgradeHelper.getCheckedMemberMapOrNull(clusterService, logger);
        if (memberMap == null) {
            // didn't pass the checks.
            return;
        }

        try {
            tryUpgradeClusterVersion(memberMap);
        } catch (OutOfMemoryError e) {
            OutOfMemoryErrorDispatcher.onOutOfMemory(e);
        } catch (Throwable throwable) {
            logger.warning(format("Scheduling new task, caught %s: %s",
                    throwable.getClass().getName(), throwable.getMessage()));
            rescheduleTaskWithBackoff();
        }
    }

    private void tryUpgradeClusterVersion(MemberMap memberMap) {
        Version nextClusterVersion = clusterService.getLocalMember().getVersion().asVersion();
        clusterService.changeClusterVersion(nextClusterVersion, memberMap);

        if (logger.isInfoEnabled()) {
            logger.info(format("Cluster version has been upgraded to %s", nextClusterVersion));
        }
    }

    private void rescheduleTaskWithBackoff() {
        // calculate delay seconds with backoff for the next attempt
        int nextAttemptNumber = attemptNumber > MAX_BACKOFF_ATTEMPT_NUMBER ? MAX_BACKOFF_ATTEMPT_NUMBER : attemptNumber + 1;
        int nextDelaySeconds = Math.max(MIN_BACKOFF_DELAY_SECONDS, random.nextInt(1 << nextAttemptNumber));

        autoUpgradeHelper.scheduleNewAutoUpgradeTask(nextAttemptNumber, nextDelaySeconds, clusterService);
    }

    @Override
    public String toString() {
        return "ClusterVersionAutoUpgradeTask{"
                + "attemptNumber=" + attemptNumber
                + '}';
    }
}
