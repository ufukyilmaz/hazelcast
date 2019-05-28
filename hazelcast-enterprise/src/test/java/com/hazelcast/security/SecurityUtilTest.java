package com.hazelcast.security;

import com.hazelcast.config.PermissionConfig;
import com.hazelcast.config.PermissionConfig.PermissionType;
import com.hazelcast.security.permission.AllPermissions;
import com.hazelcast.security.permission.AtomicLongPermission;
import com.hazelcast.security.permission.CachePermission;
import com.hazelcast.security.permission.CardinalityEstimatorPermission;
import com.hazelcast.security.permission.ClusterPermission;
import com.hazelcast.security.permission.CountDownLatchPermission;
import com.hazelcast.security.permission.DurableExecutorServicePermission;
import com.hazelcast.security.permission.ExecutorServicePermission;
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
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SecurityUtilTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    private PermissionConfig permissionConfig = new PermissionConfig(PermissionType.ALL, "myPermission", "myPrincipal");

    @Test
    public void testConstructor() {
        assertUtilityConstructor(SecurityUtil.class);
    }

    @Test
    public void testCreatePermission() {
        testCreatePermission(PermissionType.MAP, MapPermission.class);
        testCreatePermission(PermissionType.QUEUE, QueuePermission.class);
        testCreatePermission(PermissionType.ATOMIC_LONG, AtomicLongPermission.class);
        testCreatePermission(PermissionType.COUNTDOWN_LATCH, CountDownLatchPermission.class);
        testCreatePermission(PermissionType.EXECUTOR_SERVICE, ExecutorServicePermission.class);
        testCreatePermission(PermissionType.LIST, ListPermission.class);
        testCreatePermission(PermissionType.LOCK, LockPermission.class);
        testCreatePermission(PermissionType.MULTIMAP, MultiMapPermission.class);
        testCreatePermission(PermissionType.SEMAPHORE, SemaphorePermission.class);
        testCreatePermission(PermissionType.SET, SetPermission.class);
        testCreatePermission(PermissionType.TOPIC, TopicPermission.class);
        testCreatePermission(PermissionType.ID_GENERATOR, AtomicLongPermission.class);
        testCreatePermission(PermissionType.TRANSACTION, TransactionPermission.class);
        testCreatePermission(PermissionType.DURABLE_EXECUTOR_SERVICE, DurableExecutorServicePermission.class);
        testCreatePermission(PermissionType.CARDINALITY_ESTIMATOR, CardinalityEstimatorPermission.class);
        testCreatePermission(PermissionType.SCHEDULED_EXECUTOR, ScheduledExecutorPermission.class);
        testCreatePermission(PermissionType.PN_COUNTER, PNCounterPermission.class);
        testCreatePermission(PermissionType.ALL, AllPermissions.class);
        testCreatePermission(PermissionType.CACHE, CachePermission.class);
    }

    @Test
    public void testAllPermissionsAreHandled() {
        for (PermissionType type : PermissionType.values()) {
            permissionConfig.setType(type);
            // if a permission type is not handled in ServiceUtil, an IllegalArgumentException will be thrown
            SecurityUtil.createPermission(permissionConfig);
        }
    }

    private void testCreatePermission(PermissionType type, Class<?> clazz) {
        permissionConfig.setType(type);

        ClusterPermission permission = SecurityUtil.createPermission(permissionConfig);

        assertInstanceOf(clazz, permission);
    }

    @Test
    public void testSetSecureCall() {
        try {
            SecurityUtil.setSecureCall();
        } finally {
            SecurityUtil.resetSecureCall();
        }
    }

    @Test
    public void testSetSecureCall_whenSetTwice_thenThrowException() {
        try {
            SecurityUtil.setSecureCall();

            expected.expect(SecurityException.class);
            SecurityUtil.setSecureCall();
        } finally {
            SecurityUtil.resetSecureCall();
        }
    }

    @Test
    public void testResetSecureCall_whenNotSet_thenThrowException() {
        expected.expect(SecurityException.class);
        SecurityUtil.resetSecureCall();
    }

    @Test
    public void testAddressMatches() {
        assertTrue(SecurityUtil.addressMatches("127.0.0.1", "127.0.0.*"));
        assertFalse(SecurityUtil.addressMatches("192.168.0.1", "127.0.0.*"));
    }

    @Test
    public void testGetCredentialsFullName() {
        Credentials credentials = new UsernamePasswordCredentials("name", "password");
        credentials.setEndpoint("127.0.0.1:5701");

        String fullName = SecurityUtil.getCredentialsFullName(credentials);

        assertEquals("name@127.0.0.1:5701", fullName);
    }

    @Test
    public void testGetCredentialsFullName_whenNull_thenReturnNull() {
        assertNull(SecurityUtil.getCredentialsFullName(null));
    }
}
