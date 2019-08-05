package com.hazelcast.internal.cluster.impl;

import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;
import com.hazelcast.version.Version;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static com.hazelcast.internal.cluster.impl.ClusterServiceImpl.VERSION_AUTO_UPGRADE_EXECUTOR_NAME;
import static java.util.logging.Level.FINE;
import static java.util.logging.Level.INFO;

/**
 * Helper class to check pre-conditions before
 * {@code ClusterVersionAutoUpgradeTask} execution.
 */
public class ClusterVersionAutoUpgradeHelper {

    public static final String PROP_AUTO_UPGRADE_ENABLED
            = "hazelcast.cluster.version.auto.upgrade.enabled";
    public static final String PROP_AUTO_UPGRADE_MIN_CLUSTER_SIZE
            = "hazelcast.cluster.version.auto.upgrade.min.cluster.size";

    private static final boolean DEFAULT_AUTO_UPGRADE_ENABLED = false;
    private static final int DEFAULT_AUTO_UPGRADE_MIN_CLUSTER_SIZE = 1;
    /**
     * Set this property to {@code true} to enable cluster version auto upgrade,
     * otherwise, to disable it, set it to {@code false}.
     */
    private static final HazelcastProperty AUTO_UPGRADE_ENABLED
            = new HazelcastProperty(PROP_AUTO_UPGRADE_ENABLED,
            DEFAULT_AUTO_UPGRADE_ENABLED);
    /**
     * When this property is greater than 1, auto upgrade waits to reach that
     * cluster size to run.
     */
    private static final HazelcastProperty AUTO_UPGRADE_MIN_CLUSTER_SIZE
            = new HazelcastProperty(PROP_AUTO_UPGRADE_MIN_CLUSTER_SIZE,
            DEFAULT_AUTO_UPGRADE_MIN_CLUSTER_SIZE);

    public void scheduleNewAutoUpgradeTask(ClusterServiceImpl clusterService) {
        scheduleNewAutoUpgradeTask(0, 0, clusterService);
    }

    void scheduleNewAutoUpgradeTask(int attemptNumber, int delaySeconds,
                                    ClusterServiceImpl clusterService) {
        NodeEngineImpl nodeEngine = clusterService.getNodeEngine();
        ILogger logger = nodeEngine.getLogger(ClusterVersionAutoUpgradeHelper.class);
        MemberMap memberMap = getCheckedMemberMapOrNull(clusterService, logger);
        if (memberMap != null) {
            scheduleTask(attemptNumber, delaySeconds, clusterService, logger);
        }
    }

    private void scheduleTask(int attemptNumber, int delaySeconds,
                              ClusterServiceImpl clusterService, ILogger logger) {
        NodeEngineImpl nodeEngine = clusterService.getNodeEngine();
        ExecutionService executionService = nodeEngine.getExecutionService();

        Runnable task = new ClusterVersionAutoUpgradeTask(clusterService, attemptNumber, this);
        executionService.schedule(VERSION_AUTO_UPGRADE_EXECUTOR_NAME, task, delaySeconds, TimeUnit.SECONDS);

        log(logger, INFO, "Cluster version auto upgrade task has been scheduled");
    }

    /**
     * @return member-map to update cluster version after
     * doing needed checks, otherwise return {@code null} to
     * indicate checks are failed and task cannot be executed at this time.
     */
    public MemberMap getCheckedMemberMapOrNull(ClusterServiceImpl clusterService, ILogger logger) {
        HazelcastProperties properties = clusterService.getNodeEngine().getProperties();

        // Check if auto upgrade is enabled
        if (!properties.getBoolean(AUTO_UPGRADE_ENABLED)) {
            log(logger, FINE, "Auto upgrade is not enabled");
            return null;
        }

        // Check if this member is master
        if (!clusterService.isMaster()) {
            log(logger, FINE, "Auto upgrade can only be started by master member");
            return null;
        }

        // Check if we reached min cluster size to proceed
        int minClusterSize = properties.getInteger(AUTO_UPGRADE_MIN_CLUSTER_SIZE);
        MemberMap memberMap = clusterService.getMembershipManager().getMemberMap();
        int currentClusterSize = memberMap.size();
        if (currentClusterSize < minClusterSize) {
            log(logger, FINE, "Auto upgrade waits to reach minimum cluster size of "
                    + "%s to proceed, now it is %s", minClusterSize, currentClusterSize);
            return null;
        }

        // Check if all member versions are greater than current cluster version
        Version currentClusterVersion = clusterService.getClusterVersion();
        if (!areMemberVersionsGreaterThan(currentClusterVersion, memberMap)) {
            log(logger, FINE, "All member versions should "
                    + "be greater than current cluster version %s "
                    + "to proceed auto upgrade", currentClusterSize);
            return null;
        }

        return memberMap;
    }

    /**
     * Used to check whether all member versions have been upgraded
     * to a version that is greater than current cluster version.
     * This is a precondition to schedule auto upgrade task.
     *
     * @return {@code true} if cluster version upgrade task can be
     * scheduled, otherwise {@code false} to indicate rolling member
     * upgrade(RU) is in progress or all member-versions are in sync with
     * the current cluster version and no need to schedule upgrade task
     */
    private static boolean areMemberVersionsGreaterThan(Version currentClusterVersion,
                                                        MemberMap memberMap) {
        Set<MemberImpl> members = memberMap.getMembers();
        for (MemberImpl member : members) {
            Version memberVersion = member.getVersion().asVersion();
            if (memberVersion.isGreaterThan(currentClusterVersion)) {
                continue;
            }

            return false;
        }

        return true;
    }

    private static void log(ILogger logger, Level level, String msg) {
        log(logger, level, msg, null);
    }

    private static void log(ILogger logger, Level level, String msg, Object param) {
        if (logger.isLoggable(level)) {
            logger.log(level, param == null ? msg : String.format(msg, param));
        }
    }

    private static void log(ILogger logger, Level level, String msg, Object param1, Object param2) {
        if (logger.isLoggable(level)) {
            logger.log(level, String.format(msg, param1, param2));
        }
    }
}
