package com.hazelcast.security;

import com.hazelcast.cp.internal.datastructures.unsafe.idgen.IdGeneratorService;
import com.hazelcast.config.PermissionConfig;
import com.hazelcast.security.permission.AllPermissions;
import com.hazelcast.security.permission.AtomicLongPermission;
import com.hazelcast.security.permission.AtomicReferencePermission;
import com.hazelcast.security.permission.CachePermission;
import com.hazelcast.security.permission.CardinalityEstimatorPermission;
import com.hazelcast.security.permission.ClusterPermission;
import com.hazelcast.security.permission.ConfigPermission;
import com.hazelcast.security.permission.CountDownLatchPermission;
import com.hazelcast.security.permission.DurableExecutorServicePermission;
import com.hazelcast.security.permission.ExecutorServicePermission;
import com.hazelcast.security.permission.FlakeIdGeneratorPermission;
import com.hazelcast.security.permission.ListPermission;
import com.hazelcast.security.permission.LockPermission;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.security.permission.MultiMapPermission;
import com.hazelcast.security.permission.PNCounterPermission;
import com.hazelcast.security.permission.QueuePermission;
import com.hazelcast.security.permission.ScheduledExecutorPermission;
import com.hazelcast.security.permission.SemaphorePermission;
import com.hazelcast.security.permission.SetPermission;
import com.hazelcast.security.permission.TopicPermission;
import com.hazelcast.security.permission.TransactionPermission;
import com.hazelcast.security.permission.UserCodeDeploymentPermission;
import com.hazelcast.internal.util.AddressUtil;

import java.util.Set;

@SuppressWarnings("checkstyle:classdataabstractioncoupling")
public final class SecurityUtil {

    private static final ThreadLocal<Boolean> SECURE_CALL = new ThreadLocal<Boolean>();

    private SecurityUtil() {
    }

    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:returncount"})
    public static ClusterPermission createPermission(PermissionConfig permissionConfig) {
        final Set<String> actionSet = permissionConfig.getActions();
        final String[] actions = actionSet.toArray(new String[actionSet.size()]);
        switch (permissionConfig.getType()) {
            case MAP:
                return new MapPermission(permissionConfig.getName(), actions);
            case QUEUE:
                return new QueuePermission(permissionConfig.getName(), actions);
            case ATOMIC_LONG:
                return new AtomicLongPermission(permissionConfig.getName(), actions);
            case ATOMIC_REFERENCE:
                return new AtomicReferencePermission(permissionConfig.getName(), actions);
            case COUNTDOWN_LATCH:
                return new CountDownLatchPermission(permissionConfig.getName(), actions);
            case EXECUTOR_SERVICE:
                return new ExecutorServicePermission(permissionConfig.getName(), actions);
            case LIST:
                return new ListPermission(permissionConfig.getName(), actions);
            case LOCK:
                return new LockPermission(permissionConfig.getName(), actions);
            case MULTIMAP:
                return new MultiMapPermission(permissionConfig.getName(), actions);
            case SEMAPHORE:
                return new SemaphorePermission(permissionConfig.getName(), actions);
            case SET:
                return new SetPermission(permissionConfig.getName(), actions);
            case TOPIC:
                return new TopicPermission(permissionConfig.getName(), actions);
            case ID_GENERATOR:
                return new AtomicLongPermission(IdGeneratorService.ATOMIC_LONG_NAME + permissionConfig.getName(), actions);
            case FLAKE_ID_GENERATOR:
                return new FlakeIdGeneratorPermission(permissionConfig.getName(), actions);
            case TRANSACTION:
                return new TransactionPermission();
            case DURABLE_EXECUTOR_SERVICE:
                return new DurableExecutorServicePermission(permissionConfig.getName(), actions);
            case CARDINALITY_ESTIMATOR:
                return new CardinalityEstimatorPermission(permissionConfig.getName(), actions);
            case SCHEDULED_EXECUTOR:
                return new ScheduledExecutorPermission(permissionConfig.getName(), actions);
            case PN_COUNTER:
                return new PNCounterPermission(permissionConfig.getName(), actions);
            case ALL:
                return new AllPermissions();
            case CACHE:
                return new CachePermission(permissionConfig.getName(), actions);
            case USER_CODE_DEPLOYMENT:
                return new UserCodeDeploymentPermission(actions);
            case CONFIG:
                return new ConfigPermission();
            default:
                throw new IllegalArgumentException(permissionConfig.getType().toString());
        }
    }

    static void setSecureCall() {
        if (isSecureCall()) {
            throw new SecurityException("Not allowed! <SECURE_CALL> flag is already set!");
        }
        SECURE_CALL.set(Boolean.TRUE);
    }

    static void resetSecureCall() {
        if (!isSecureCall()) {
            throw new SecurityException("Not allowed! <SECURE_CALL> flag is not set!");
        }
        SECURE_CALL.remove();
    }

    private static boolean isSecureCall() {
        final Boolean value = SECURE_CALL.get();
        return value != null && value;
    }

    public static boolean addressMatches(String address, String pattern) {
        return AddressUtil.matchInterface(address, pattern);
    }
}
