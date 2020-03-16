package com.hazelcast.client.security;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.LoginModuleConfig;
import com.hazelcast.config.LoginModuleConfig.LoginModuleUsage;
import com.hazelcast.config.PermissionConfig;
import com.hazelcast.config.PermissionConfig.PermissionType;
import com.hazelcast.config.security.JaasAuthenticationConfig;
import com.hazelcast.config.security.RealmConfig;
import com.hazelcast.config.security.TokenIdentityConfig;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.security.loginmodules.TestLoginModule;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Properties;

import static com.hazelcast.config.PermissionConfig.PermissionType.ALL;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientSecurityTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final TestHazelcastFactory factory = new TestHazelcastFactory();
    private final String testObjectName = randomString();

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Test
    public void testDenyAll() {
        final Config config = createConfig();
        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();
        expectedException.expect(RuntimeException.class);
        client.getMap(testObjectName).size();
    }

    @Test
    public void testAllowAll() {
        final Config config = createConfig();
        addPermission(config, ALL, "", null);

        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();
        client.getMap(testObjectName).size();
        client.getMap(testObjectName).size();
        client.getMap(testObjectName).put("a", "b");
        client.getQueue("Q").poll();
        client.getReliableTopic(testObjectName).publish("m");
        client.getRingbuffer(testObjectName).add("s");
    }

    @Test
    public void testShortToken() {
        Properties properties = new Properties();
        properties.setProperty(TestLoginModule.PROPERTY_PRINCIPALS_ROLE, "admin");
        properties.setProperty(TestLoginModule.PROPERTY_PRINCIPALS_IDENTITY, "josef");
        Config config = createTestLoginModuleConfig(properties);
        addPermission(config, ALL, "", null);
        factory.newHazelcastInstance(config);
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getSecurityConfig().setTokenIdentityConfig(new TokenIdentityConfig(new byte[1]));
        HazelcastInstance client = factory.newHazelcastClient(clientConfig);
        client.getMap(testObjectName).size();
    }

    @Test
    public void testDenyEndpoint() {
        final Config config = createConfig();
        final PermissionConfig pc = addPermission(config, ALL, "", "dev");
        pc.addEndpoint("10.10.10.*");

        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();
        expectedException.expect(RuntimeException.class);
        client.getMap(testObjectName).size();
    }

    /**
     * Tests multiple principals in JAAS Subject.
     * <pre>
     * Given: Member has configured permissions for 2 Maps
     *   - "production" with permissions for "admin" and "dev" principals
     *   - testObjectName with permissions for testObjectName principal
     * When: Client joins and has Subject with 3 JAAS principals:
     *   - "admin" ClusterPrincipal
     *   - "dev" ClusterPrincipal
     *   - testObjectName instance of Principal, which is not ClusterPrincipal
     * Then: the client has all the mapped permissions for "admin" and "dev", but it does not get permissions mapped for the testObjectName
     * </pre>
     */
    @Test
    public void testMultiplePrincipalsInSubject() {
        Properties properties = new Properties();
        properties.setProperty(TestLoginModule.PROPERTY_PRINCIPALS_SIMPLE, testObjectName);
        properties.setProperty(TestLoginModule.PROPERTY_PRINCIPALS_ROLE, "dev,admin");
        properties.setProperty(TestLoginModule.PROPERTY_PRINCIPALS_IDENTITY, "josef");
        final Config config = createTestLoginModuleConfig(properties);
        addPermission(config, PermissionType.MAP, "production", "admin,dev")
                .addAction(ActionConstants.ACTION_CREATE);
        addPermission(config, PermissionType.MAP, "production", "dev")
                .addAction(ActionConstants.ACTION_READ)
                .addAction(ActionConstants.ACTION_PUT);
        addPermission(config, PermissionType.MAP, "production", "admin")
                .addAction(ActionConstants.ACTION_REMOVE);
        addPermission(config, PermissionType.MAP, testObjectName, testObjectName)
                .addAction(ActionConstants.ACTION_CREATE)
                .addAction(ActionConstants.ACTION_PUT)
                .addAction(ActionConstants.ACTION_READ)
                .addAction(ActionConstants.ACTION_REMOVE);

        factory.newHazelcastInstance(config);

        HazelcastInstance client = factory.newHazelcastClient();
        IMap<String, String> map = client.getMap("production");
        assertNull(map.put("1", "A"));
        assertEquals("A", map.get("1"));
        assertEquals("A", map.remove("1"));
        try {
            map.lock("1");
            fail("Lock operation on 'production' IMap should be denied.");
        } catch (RuntimeException e) {
            // expected
        }

        try {
            client.getMap(testObjectName);
            fail("Create operation on 'test' IMap should be denied.");
        } catch (RuntimeException e) {
            // expected
        }
    }

    private Config createConfig() {
        final Config config = new Config();
        final SecurityConfig secCfg = config.getSecurityConfig();
        secCfg.setEnabled(true);
        return config;
    }

    private PermissionConfig addPermission(Config config, PermissionType type, String name, String principal) {
        PermissionConfig perm = new PermissionConfig(type, name, principal);
        config.getSecurityConfig().addClientPermissionConfig(perm);
        return perm;
    }

    /**
     * Creates member configuration with security enabled and custom client login module.
     *
     * @param properties properties of the {@link TestLoginModule} used for clients (see constants in {@link TestLoginModule}
     *                   for the property names)
     */
    private Config createTestLoginModuleConfig(Properties properties) {
        final Config config = smallInstanceConfig();
        final SecurityConfig secCfg = config.getSecurityConfig();
        secCfg.setEnabled(true);
        LoginModuleConfig loginModuleConfig = new LoginModuleConfig();
        loginModuleConfig.setClassName(TestLoginModule.class.getName());
        loginModuleConfig.setUsage(LoginModuleUsage.REQUIRED);
        loginModuleConfig.setProperties(properties);
        RealmConfig realmConfig = new RealmConfig().setJaasAuthenticationConfig(new JaasAuthenticationConfig().addLoginModuleConfig(loginModuleConfig));
        secCfg.setClientRealmConfig("clientRealm", realmConfig);
        return config;
    }
}
