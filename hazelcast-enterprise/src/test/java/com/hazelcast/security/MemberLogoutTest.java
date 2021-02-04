package com.hazelcast.security;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import javax.security.auth.login.LoginException;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.config.Config;
import com.hazelcast.config.LoginModuleConfig;
import com.hazelcast.config.LoginModuleConfig.LoginModuleUsage;
import com.hazelcast.config.security.JaasAuthenticationConfig;
import com.hazelcast.config.security.RealmConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestAwareInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({ QuickTest.class, ParallelJVMTest.class })
public class MemberLogoutTest extends HazelcastTestSupport {

    private final TestAwareInstanceFactory factory = new TestAwareInstanceFactory();

    @After
    public void after() {
        factory.terminateAll();
    }

    @Test
    public void testLogoutCalledWhenShutdown() {
        Config config = smallInstanceConfig();
        RealmConfig realmConfig = new RealmConfig().setJaasAuthenticationConfig(new JaasAuthenticationConfig()
                .addLoginModuleConfig(new LoginModuleConfig(VerifyLogoutModule.class.getName(), LoginModuleUsage.REQUIRED)));
        config.getSecurityConfig().setEnabled(true).setMemberRealmConfig("memberRealm", realmConfig);

        factory.newHazelcastInstance(config);
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        assertFalse(VerifyLogoutModule.logoutCalled);
        hz.shutdown();
        assertTrueEventually(() -> assertTrue(VerifyLogoutModule.logoutCalled));
    }

    @Test
    public void testLogoutCalledWhenTerminate() {
        Config config = smallInstanceConfig();
        RealmConfig realmConfig = new RealmConfig().setJaasAuthenticationConfig(new JaasAuthenticationConfig()
                .addLoginModuleConfig(new LoginModuleConfig(VerifyLogoutModule.class.getName(), LoginModuleUsage.REQUIRED)));
        config.getSecurityConfig().setEnabled(true).setMemberRealmConfig("memberRealm", realmConfig);
        factory.newHazelcastInstance(config);
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        assertFalse(VerifyLogoutModule.logoutCalled);
        hz.getLifecycleService().terminate();
        assertTrueEventually(() -> assertTrue(VerifyLogoutModule.logoutCalled));
    }

    public static class VerifyLogoutModule extends ClusterLoginModule {
        public static volatile boolean logoutCalled = false;

        @Override
        protected String getName() {
            return "test";
        }

        @Override
        protected boolean onLogin() throws LoginException {
            logoutCalled = false;
            return true;
        }

        @Override
        protected boolean onLogout() throws LoginException {
            logoutCalled = true;
            return true;
        }
    }
}
