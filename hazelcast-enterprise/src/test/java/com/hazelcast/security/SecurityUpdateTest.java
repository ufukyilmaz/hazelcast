package com.hazelcast.security;

import com.hazelcast.config.Config;
import com.hazelcast.config.PermissionConfig;
import com.hazelcast.config.PermissionConfig.PermissionType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.internal.management.request.UpdatePermissionConfigRequest;
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
import com.hazelcast.security.permission.QueuePermission;
import com.hazelcast.security.permission.ScheduledExecutorPermission;
import com.hazelcast.security.permission.SemaphorePermission;
import com.hazelcast.security.permission.SetPermission;
import com.hazelcast.security.permission.TopicPermission;
import com.hazelcast.security.permission.TransactionPermission;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class SecurityUpdateTest extends HazelcastTestSupport {

    @Test
    public void mapPermissionConfigUpdateTest() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        Config config = new Config();
        config.getSecurityConfig().setEnabled(true);
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        HazelcastInstance instance2 = factory.newHazelcastInstance(config);

        instance1.getMap("test").put("1", "1");


        Set<PermissionConfig> permissionConfigs = new HashSet<PermissionConfig>();
        PermissionConfig permissionConfig = new PermissionConfig();
        permissionConfig.setType(PermissionConfig.PermissionType.MAP);
        permissionConfig.setName("test");
        Set<String> actions = new HashSet<String>();
        actions.add("read");
        Set<String> endpoints = new HashSet<String>();
        endpoints.add("*");
        permissionConfig.setActions(actions);
        permissionConfig.setEndpoints(endpoints);
        permissionConfig.setPrincipal("*");
        permissionConfigs.add(permissionConfig);

        instance1.getConfig().getSecurityConfig().setClientPermissionConfigs(permissionConfigs);
        instance1.getMap("test").put("1", "1");


    }
}
