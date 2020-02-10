package com.hazelcast.client.security;

import static com.hazelcast.config.PermissionConfig.PermissionType.ALL;
import static com.hazelcast.test.Accessors.getClusterService;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import javax.security.auth.login.LoginException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.client.test.TestAwareClientFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.LoginModuleConfig;
import com.hazelcast.config.LoginModuleConfig.LoginModuleUsage;
import com.hazelcast.config.security.JaasAuthenticationConfig;
import com.hazelcast.config.security.RealmConfig;
import com.hazelcast.config.PermissionConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.internal.util.RuntimeAvailableProcessors;
import com.hazelcast.security.ClusterLoginModule;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;

/**
 * Regression tests for blocking client authentications. It should not lead to a cluster split-brain.
 * The scenario:
 * <pre>
 * - start 2 members
 * - start 50 client threads which are blocked during the authentication
 * - do several IMap.put operations
 * - start 3rd member
 * - check the cluster size is 3 and members list version is 3 (or lower)
 * </pre>
 */
@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category(QuickTest.class)
public class ClientAuthnSplitBrainTest {

    private final TestAwareClientFactory factory = new TestAwareClientFactory();

    @BeforeClass
    public static void beforeClass() {
        RuntimeAvailableProcessors.override(2);
    }

    @AfterClass
    public static void afterClass() {
        RuntimeAvailableProcessors.resetOverride();
    }

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Test(timeout = 60000L)
    public void testBlockedClientAuthentication() throws InterruptedException {
        final Config config = new Config();
        RealmConfig realmConfig = new RealmConfig()
                .setJaasAuthenticationConfig(new JaasAuthenticationConfig().addLoginModuleConfig(new LoginModuleConfig()
                        .setClassName(TestLoginModule.class.getName()).setUsage(LoginModuleUsage.REQUIRED)));
        config.getSecurityConfig()
            .setEnabled(true)
            .setClientRealmConfig("realm", realmConfig)
            .addClientPermissionConfig(new PermissionConfig(ALL, "", null));

        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);

        Thread[] threads = new Thread[50];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        factory.newHazelcastClient(null).getMap("production").size();
                    } catch (Exception e) {
                    }
                }
            });
            threads[i].start();
        }
        Thread.sleep(5000L);
        for (int i = 0; i < 100; i++) {
            hz1.getMap("test").put(i, i);
        }
        HazelcastInstance hz3 = factory.newHazelcastInstance(config);
        HazelcastTestSupport.assertClusterSize(3, hz1, hz2, hz3);
        assertTrue(getClusterService(hz1).getMemberListVersion() <= 3);
    }

    public static class TestLoginModule extends ClusterLoginModule {

        @Override
        protected boolean onLogin() throws LoginException {
            try {
                TimeUnit.MINUTES.sleep(5);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return true;
        }

        @Override
        protected String getName() {
            return null;
        }
    }
}
