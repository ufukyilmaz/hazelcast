package com.hazelcast.client.bluegreen;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientFailoverConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.config.ClientSecurityConfig;
import com.hazelcast.client.impl.ClientSelectors;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.config.Config;
import com.hazelcast.config.CredentialsFactoryConfig;
import com.hazelcast.config.LoginModuleConfig;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.config.security.JaasAuthenticationConfig;
import com.hazelcast.config.security.RealmConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.cluster.Member;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.enterprise.SampleLicense;
import com.hazelcast.cluster.Address;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.CredentialsCallback;
import com.hazelcast.security.ICredentialsFactory;
import com.hazelcast.security.TokenDeserializerCallback;
import com.hazelcast.security.TokenCredentials;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class ClustersWithDifferentCredentialsTest extends ClientTestSupport {

    @After
    public void cleanUp() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    private Member toMember(HazelcastInstance instance1) {
        return (Member) instance1.getLocalEndpoint();
    }

    static class CustomCredentials implements Credentials {

        private String secret;

        CustomCredentials(String secret) {
            this.secret = secret;
        }

        @Override
        public String getName() {
            return secret;
        }
    }

    public static class CustomLoginModule implements LoginModule {

        CallbackHandler callbackHandler;
        Subject subject;
        Map<String, ?> options;

        public void initialize(Subject subject, CallbackHandler callbackHandler,
                               Map<String, ?> sharedState, Map<String, ?> options) {
            this.subject = subject;
            this.callbackHandler = callbackHandler;
            this.options = options;
        }

        public final boolean login() throws LoginException {
            CredentialsCallback callback = new CredentialsCallback();
            TokenDeserializerCallback tdcallback = new TokenDeserializerCallback();
            try {
                callbackHandler.handle(new Callback[]{tdcallback, callback});
                Credentials credentials = callback.getCredentials();
                if (credentials instanceof TokenCredentials) {
                    TokenCredentials tokenCreds = (TokenCredentials) credentials;
                    credentials = (Credentials) tdcallback.getTokenDeserializer().deserialize(tokenCreds);
                }
                if (credentials.getName().equals(options.get("secret"))) {
                    return true;
                }
            } catch (Exception e) {
                throw new LoginException(e.getMessage());
            }
            throw new LoginException();
        }

        @Override
        public boolean commit() throws LoginException {
            return true;
        }

        @Override
        public boolean abort() throws LoginException {
            return true;
        }

        @Override
        public boolean logout() throws LoginException {
            return true;
        }
    }

    public static class CustomCredentialsFactory implements ICredentialsFactory {

        private String secret;

        @Override
        public void init(Properties properties) {
            secret = properties.getProperty("secret");
        }

        @Override
        public void configure(CallbackHandler callbackHandler) {
        }

        @Override
        public Credentials newCredentials() {
            return new CustomCredentials(secret);
        }

        @Override
        public void destroy() {

        }
    }

    @Test
    public void test_betweenTwoSecurityEnabled() {
        Config config1 = new Config();
        config1.setLicenseKey(SampleLicense.UNLIMITED_LICENSE);
        LoginModuleConfig loginModuleConfig1 = new LoginModuleConfig();
        loginModuleConfig1.getProperties().setProperty("secret", "cluster1");
        loginModuleConfig1.setClassName(CustomLoginModule.class.getName());
        loginModuleConfig1.setUsage(LoginModuleConfig.LoginModuleUsage.REQUIRED);
        SecurityConfig securityConfig1 = config1.getSecurityConfig();
        securityConfig1.setEnabled(true);
        RealmConfig realmConfig1 = new RealmConfig();
        realmConfig1.setJaasAuthenticationConfig(new JaasAuthenticationConfig().addLoginModuleConfig(loginModuleConfig1));
        securityConfig1.setClientRealmConfig("realm", realmConfig1);
        config1.setClusterName("dev1");
        config1.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config1.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config1);

        Config config2 = new Config();
        config2.setLicenseKey(SampleLicense.UNLIMITED_LICENSE);
        LoginModuleConfig loginModuleConfig2 = new LoginModuleConfig();
        loginModuleConfig2.getProperties().setProperty("secret", "cluster2");
        loginModuleConfig2.setClassName(CustomLoginModule.class.getName());
        loginModuleConfig2.setUsage(LoginModuleConfig.LoginModuleUsage.REQUIRED);
        SecurityConfig securityConfig2 = config2.getSecurityConfig();
        securityConfig2.setEnabled(true);
        RealmConfig realmConfig2 = new RealmConfig();
        realmConfig2.setJaasAuthenticationConfig(new JaasAuthenticationConfig().addLoginModuleConfig(loginModuleConfig2));
        securityConfig2.setClientRealmConfig("realm", realmConfig2);
        config2.setClusterName("dev2");
        config2.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config2.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config2);

        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName("dev1");
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        Member member1 = toMember(instance1);
        Address address1 = member1.getAddress();
        networkConfig.setAddresses(Collections.singletonList(address1.getHost() + ":" + address1.getPort()));

        CredentialsFactoryConfig credentialsFactoryConfig1 = new CredentialsFactoryConfig(
                CustomCredentialsFactory.class.getName());
        credentialsFactoryConfig1.getProperties().setProperty("secret", "cluster1");

        ClientSecurityConfig clientSecurityConfig1 = clientConfig.getSecurityConfig();
        clientSecurityConfig1.setCredentialsFactoryConfig(credentialsFactoryConfig1);

        ClientConfig clientConfig2 = new ClientConfig();
        clientConfig2.setClusterName("dev2");
        Member member2 = toMember(instance2);
        Address address2 = member2.getAddress();
        ClientNetworkConfig networkConfig2 = clientConfig2.getNetworkConfig();
        networkConfig2.setAddresses(Collections.singletonList(address2.getHost() + ":" + address2.getPort()));

        CredentialsFactoryConfig credentialsFactoryConfig2 = new CredentialsFactoryConfig();
        credentialsFactoryConfig2.setClassName(CustomCredentialsFactory.class.getName());
        credentialsFactoryConfig2.getProperties().setProperty("secret", "cluster2");
        ClientSecurityConfig clientSecurityConfig2 = new ClientSecurityConfig();
        clientSecurityConfig2.setCredentialsFactoryConfig(credentialsFactoryConfig2);
        clientConfig2.setSecurityConfig(clientSecurityConfig2);

        ClientFailoverConfig clientFailoverConfig = new ClientFailoverConfig();
        clientFailoverConfig.addClientConfig(clientConfig).addClientConfig(clientConfig2);
        HazelcastInstance client = HazelcastClient.newHazelcastFailoverClient(clientFailoverConfig);

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        client.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (LifecycleEvent.LifecycleState.CLIENT_CHANGED_CLUSTER.equals(event.getState())) {
                    countDownLatch.countDown();
                }
            }
        });

        Set<Member> members = client.getCluster().getMembers();
        assertEquals(1, members.size());
        assertContains(members, member1);

        getClientEngineImpl(instance1).applySelector(ClientSelectors.none());

        assertOpenEventually(countDownLatch);

        members = client.getCluster().getMembers();
        assertEquals(1, members.size());
        assertContains(members, member2);
    }

    @Test
    public void test_migrationToSecurityEnabled() {
        Config config1 = new Config();
        config1.setClusterName("dev1");
        config1.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config1.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config1);

        Config config2 = new Config();
        config2.setLicenseKey(SampleLicense.UNLIMITED_LICENSE);
        LoginModuleConfig loginModuleConfig2 = new LoginModuleConfig();
        loginModuleConfig2.getProperties().setProperty("secret", "cluster2");
        loginModuleConfig2.setClassName(CustomLoginModule.class.getName());
        loginModuleConfig2.setUsage(LoginModuleConfig.LoginModuleUsage.REQUIRED);
        SecurityConfig securityConfig2 = config2.getSecurityConfig();
        securityConfig2.setEnabled(true);
        RealmConfig realmConfig = new RealmConfig();
        realmConfig.setJaasAuthenticationConfig(new JaasAuthenticationConfig().addLoginModuleConfig(loginModuleConfig2));
        securityConfig2.setClientRealmConfig("realm", realmConfig);
        config2.setClusterName("dev2");
        config2.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config2.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config2);

        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName("dev1");
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        Member member1 = toMember(instance1);
        Address address1 = member1.getAddress();
        networkConfig.setAddresses(Collections.singletonList(address1.getHost() + ":" + address1.getPort()));

        ClientConfig clientConfig2 = new ClientConfig();
        clientConfig2.setClusterName("dev2");
        Member member2 = toMember(instance2);
        Address address2 = member2.getAddress();
        ClientNetworkConfig networkConfig2 = clientConfig2.getNetworkConfig();
        networkConfig2.setAddresses(Collections.singletonList(address2.getHost() + ":" + address2.getPort()));

        CredentialsFactoryConfig credentialsFactoryConfig2 = new CredentialsFactoryConfig();
        credentialsFactoryConfig2.setClassName(CustomCredentialsFactory.class.getName());
        credentialsFactoryConfig2.getProperties().setProperty("secret", "cluster2");
        ClientSecurityConfig clientSecurityConfig2 = new ClientSecurityConfig();
        clientSecurityConfig2.setCredentialsFactoryConfig(credentialsFactoryConfig2);
        clientConfig2.setSecurityConfig(clientSecurityConfig2);

        ClientFailoverConfig clientFailoverConfig = new ClientFailoverConfig();
        clientFailoverConfig.addClientConfig(clientConfig).addClientConfig(clientConfig2);
        HazelcastInstance client = HazelcastClient.newHazelcastFailoverClient(clientFailoverConfig);

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        client.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (LifecycleEvent.LifecycleState.CLIENT_CHANGED_CLUSTER.equals(event.getState())) {
                    countDownLatch.countDown();
                }
            }
        });
        Set<Member> members = client.getCluster().getMembers();
        assertEquals(1, members.size());
        assertContains(members, member1);

        getClientEngineImpl(instance1).applySelector(ClientSelectors.none());

        assertOpenEventually(countDownLatch);

        members = client.getCluster().getMembers();
        assertEquals(1, members.size());
        assertContains(members, member2);
    }

    @Test
    public void test_migrationFromSecurityEnabled() {
        Config config1 = new Config();
        config1.setLicenseKey(SampleLicense.UNLIMITED_LICENSE);
        LoginModuleConfig loginModuleConfig1 = new LoginModuleConfig();
        loginModuleConfig1.getProperties().setProperty("secret", "cluster1");
        loginModuleConfig1.setClassName(CustomLoginModule.class.getName());
        loginModuleConfig1.setUsage(LoginModuleConfig.LoginModuleUsage.REQUIRED);
        SecurityConfig securityConfig1 = config1.getSecurityConfig();
        securityConfig1.setEnabled(true);
        RealmConfig realmConfig = new RealmConfig();
        realmConfig.setJaasAuthenticationConfig(new JaasAuthenticationConfig().addLoginModuleConfig(loginModuleConfig1));
        securityConfig1.setClientRealmConfig("realm", realmConfig);
        config1.setClusterName("dev1");
        config1.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config1.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config1);

        Config config2 = new Config();
        config2.setClusterName("dev2");
        config2.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config2.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config2);

        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName("dev1");
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        Member member1 = toMember(instance1);
        Address address1 = member1.getAddress();
        networkConfig.setAddresses(Collections.singletonList(address1.getHost() + ":" + address1.getPort()));

        CredentialsFactoryConfig credentialsFactoryConfig1 = new CredentialsFactoryConfig();
        credentialsFactoryConfig1.setClassName(CustomCredentialsFactory.class.getName());
        credentialsFactoryConfig1.getProperties().setProperty("secret", "cluster1");

        ClientSecurityConfig clientSecurityConfig1 = clientConfig.getSecurityConfig();
        clientSecurityConfig1.setCredentialsFactoryConfig(credentialsFactoryConfig1);

        ClientConfig clientConfig2 = new ClientConfig();
        clientConfig2.setClusterName("dev2");
        Member member2 = toMember(instance2);
        Address address2 = member2.getAddress();
        ClientNetworkConfig networkConfig2 = clientConfig2.getNetworkConfig();
        networkConfig2.setAddresses(Collections.singletonList(address2.getHost() + ":" + address2.getPort()));

        ClientFailoverConfig clientFailoverConfig = new ClientFailoverConfig();
        clientFailoverConfig.addClientConfig(clientConfig).addClientConfig(clientConfig2);
        HazelcastInstance client = HazelcastClient.newHazelcastFailoverClient(clientFailoverConfig);

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        client.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (LifecycleEvent.LifecycleState.CLIENT_CHANGED_CLUSTER.equals(event.getState())) {
                    countDownLatch.countDown();
                }
            }
        });
        Set<Member> members = client.getCluster().getMembers();
        assertEquals(1, members.size());
        assertContains(members, member1);

        getClientEngineImpl(instance1).applySelector(ClientSelectors.none());

        assertOpenEventually(countDownLatch);

        members = client.getCluster().getMembers();
        assertEquals(1, members.size());
        assertContains(members, member2);
    }

}
