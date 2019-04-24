package com.hazelcast.client.security;

import static com.hazelcast.config.MapStoreConfig.InitialLoadMode.LAZY;
import static com.hazelcast.config.PermissionConfig.PermissionType.ALL;
import static com.hazelcast.core.Hazelcast.getHazelcastInstanceByName;
import static com.hazelcast.test.AbstractHazelcastClassRunner.getTestMethodName;
import static com.hazelcast.test.HazelcastTestSupport.assertClusterSize;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.LoginModuleConfig;
import com.hazelcast.config.LoginModuleConfig.LoginModuleUsage;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.config.MaxSizeConfig.MaxSizePolicy;
import com.hazelcast.config.PermissionConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapLoader;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.jmx.ManagementService;
import com.hazelcast.internal.util.RuntimeAvailableProcessors;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.security.ClusterPrincipal;
import com.hazelcast.security.CredentialsCallback;
import com.hazelcast.security.UsernamePasswordCredentials;
import com.hazelcast.test.TestAwareInstanceFactory;
import com.hazelcast.test.annotation.NightlyTest;

/**
 * Regression tests for blocking client authentications. It should not lead to a cluster split-brain.
 * The Hazelcast challenging part in this test is working with IMap within a LoginModule. The IMap has configured
 * a {@link MapLoader}.
 * Scenario:
 * <pre>
 * - start 3 members (with 30 seconds configured as suspect timeout)
 * - start 20 client threads and let them do some work;
 *   half of the clients is long lived and other half is only used shortly and then recreated
 * - after 40 seconds waiting check the cluster size
 * <pre>
 */
@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({ NightlyTest.class })
public class ClientAuthnUsingMapCacheTest {

    private final TestAwareClientFactory factory = new TestAwareClientFactory();

    public static final int LOGIN_MAP_SIZE = 100000;
    public static final String OPT_INSTANCE_NAME = "instanceName";
    private static final String USER_MAP = "userMap";

    @BeforeClass
    public static void beforeClass() {
        RuntimeAvailableProcessors.override(2);
    }

    @AfterClass
    public static void afterClass() {
        RuntimeAvailableProcessors.resetOverride();
    }

    @After
    public void after() {
        factory.terminateAll();
    }

    @Test(timeout = 120000)
    public void testClientAuthentication() throws InterruptedException {
        HazelcastInstance hz = createMember(0);
        IMap<Integer, byte[]> xMap = hz.getMap("xMap");
        Random rnd = new Random();
        while (xMap.size() < 1000) {
            xMap.put(rnd.nextInt(), new byte[1024 * 128]);
        }

        Thread[] memberThreads = new Thread[2];
        for (int i = 0; i < memberThreads.length; i++) {
            final int idx = i + 1;
            memberThreads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    createMember(idx);
                }
            });
            memberThreads[i].start();
        }
        for (int i = 0; i < memberThreads.length; i++) {
            memberThreads[i].join();
        }

        Thread[] clientThreads = new Thread[20];
        for (int i = 0; i < clientThreads.length; i++) {
            clientThreads[i] = new Thread(new ClientRunnable(i, factory));
            clientThreads[i].start();
        }
        TimeUnit.SECONDS.sleep(40);
        assertClusterSize(3, hz, getHazelcastInstanceByName("test1"), getHazelcastInstanceByName("test2"));
    }

    private HazelcastInstance createMember(int idx) {
        String instanceName = "test" + idx;
        final Config config = new Config().setInstanceName(instanceName).setProperty("hazelcast.max.no.heartbeat.seconds",
                "30");
        MapConfig mapConfig = new MapConfig().setName(USER_MAP)
                .setMapStoreConfig(new MapStoreConfig().setEnabled(true).setInitialLoadMode(LAZY)
                        .setClassName(UserCredentialsMapLoader.class.getName()))
                .setBackupCount(2).setReadBackupData(true).setEvictionPolicy(EvictionPolicy.LFU)
                .setMaxSizeConfig(new MaxSizeConfig(100, MaxSizePolicy.PER_NODE));
        config.addMapConfig(mapConfig);
        LoginModuleConfig loginModuleConfig = new LoginModuleConfig().setClassName(ClientLoginModule.class.getName())
                .setUsage(LoginModuleUsage.REQUIRED);
        loginModuleConfig.getProperties().setProperty(OPT_INSTANCE_NAME, instanceName);
        config.getSecurityConfig().setEnabled(true).addClientLoginModuleConfig(loginModuleConfig)
                .addClientPermissionConfig(new PermissionConfig(ALL, "", null));

        return factory.newHazelcastInstance(config);
    }

    @SuppressWarnings("deprecation")
    public static ClientConfig createClientConfig(String id) {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getGroupConfig().setName(id).setPassword(id);
        return clientConfig;
    }

    public static class UserCredentialsMapLoader implements MapLoader<String, String> {

        @Override
        public String load(String key) {
            try {
                int id = Integer.parseInt(key);
                if (id < 0 || id >= LOGIN_MAP_SIZE) {
                    return null;
                }
            } catch (NumberFormatException e) {
                return null;
            }
            return key;
        }

        @Override
        public Map<String, String> loadAll(Collection<String> keys) {
            Map<String, String> hashMap = new HashMap<String, String>();
            for (String key : keys) {
                String cred = load(key);
                if (cred != null) {
                    hashMap.put(key, cred);
                }
            }
            return hashMap;
        }

        @Override
        public Iterable<String> loadAllKeys() {
            List<String> list = new ArrayList<String>(LOGIN_MAP_SIZE);
            for (int i = 0; i < LOGIN_MAP_SIZE; i++) {
                list.add(String.valueOf(i));
            }
            return list;
        }
    }

    public static class ClientLoginModule implements LoginModule {

        private ILogger LOGGER = Logger.getLogger(ClientLoginModule.class);

        private Subject subject;
        private CallbackHandler callbackHandler;
        private String userName;
        private UsernamePasswordCredentials usernamePasswordCredentials;
        private String instanceName;

        @Override
        public void initialize(Subject subject, CallbackHandler callbackHandler, Map<String, ?> sharedState,
                Map<String, ?> options) {
            this.subject = subject;
            this.callbackHandler = callbackHandler;
            instanceName = (String) options.get(OPT_INSTANCE_NAME);
        }

        @Override
        public boolean login() throws LoginException {
            final CredentialsCallback cb = new CredentialsCallback();
            try {
                callbackHandler.handle(new Callback[] { cb });
                usernamePasswordCredentials = (UsernamePasswordCredentials) cb.getCredentials();
            } catch (Exception e) {
                throw new LoginException(e.getClass().getName() + ":" + e.getMessage());
            }
            try {
                HazelcastInstance hazelcastInstance = Hazelcast.getHazelcastInstanceByName(instanceName);
                IMap<String, String> userCredentialsMap = hazelcastInstance.getMap(USER_MAP);
                userName = userCredentialsMap.get(usernamePasswordCredentials.getUsername());
            } catch (Exception e) {
                LOGGER.warning("IMap Authentication failed", e);
            }
            if (null == userName) {
                LOGGER.info("Emulating DB authentication.");
                try {
                    Thread.sleep(500L);
                } catch (InterruptedException e) {
                }
                userName = usernamePasswordCredentials.getUsername();
            }

            if (!usernamePasswordCredentials.getUsername().equals(userName)
                    || !usernamePasswordCredentials.getPassword().equals(userName)) {
                userName = null;
                throw new FailedLoginException();
            }
            return true;
        }

        @Override
        public boolean commit() throws LoginException {
            if (userName != null) {
                subject.getPrincipals().add(new ClusterPrincipal(usernamePasswordCredentials));
            }
            return true;
        }

        @Override
        public boolean abort() throws LoginException {
            subject.getPrincipals().clear();
            return true;
        }

        @Override
        public boolean logout() throws LoginException {
            subject.getPrincipals().clear();
            return true;
        }
    }

    public static class ClientRunnable implements Runnable {
        private final int clientId;
        private final TestAwareClientFactory factory;

        public ClientRunnable(int clientId, TestAwareClientFactory factory) {
            this.clientId = clientId;
            this.factory = factory;
        }

        public void run() {
            final Random random = new Random();
            HazelcastInstance client = null;
            //the test timeout is 2min
            long endTime = System.currentTimeMillis() + 120000;
            while (System.currentTimeMillis() < endTime) {
                try {
                    if (client == null) {
                        client = factory.newHazelcastClient(createClientConfig(String.valueOf(random.nextInt(LOGIN_MAP_SIZE))));
                    }
                    for (int i = 0; i < 1000; i++) {
                        client.getMap("test").put(random.nextInt(1000), random.nextInt());
                        client.getMap("xMap").get(random.nextInt());
                        Thread.sleep(10);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    // half of the threads will be with shortliving clients
                    if (client != null && clientId % 2 == 0) {
                        factory.terminateClient(client);
                        client = null;
                    }
                }
            }
        }
    }

    /**
     * Custom client instance factory
     */
    public static class TestAwareClientFactory extends TestAwareInstanceFactory {

        protected final Map<String, List<HazelcastInstance>> perMethodClients = new ConcurrentHashMap<String, List<HazelcastInstance>>();

        public HazelcastInstance newHazelcastClient(ClientConfig config) {
            List<HazelcastInstance> members = getOrInitInstances(perMethodMembers);
            if (members.isEmpty()) {
                throw new IllegalStateException("Members have to be created first");
            }
            ClientNetworkConfig networkConfig = config.getNetworkConfig();
            for (HazelcastInstance member : members) {
                networkConfig.addAddress("127.0.0.1:" + getPort(member, EndpointQualifier.CLIENT));
            }
            HazelcastInstance hz = HazelcastClient.newHazelcastClient(config);
            getOrInitInstances(perMethodClients).add(hz);
            return hz;
        }

        /**
         * Terminates all client and member instances created by this factory for current test method name.
         */
        @Override
        public void terminateAll() {
            try {
                shutdownInstances(perMethodClients.remove(getTestMethodName()));
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                super.terminateAll();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        /**
         * Terminates given client instance created by this factory.
         */
        public void terminateClient(HazelcastInstance hz) {
            try {
                ManagementService.shutdown(hz.getName());
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                hz.getLifecycleService().terminate();
            } catch (Exception e) {
                e.printStackTrace();
            }
            getOrInitInstances(perMethodClients).remove(hz);
        }
    }
}
