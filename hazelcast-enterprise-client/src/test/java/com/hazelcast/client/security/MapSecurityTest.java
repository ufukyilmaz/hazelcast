package com.hazelcast.client.security;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.LoginModuleConfig;
import com.hazelcast.config.PermissionConfig;
import com.hazelcast.config.PermissionConfig.PermissionType;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.AccessControlException;
import java.util.Properties;

import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapSecurityTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private static final String HAZELCAST_START_TAG = "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n";
    private static final String HAZELCAST_END_TAG = "</hazelcast>\n";

    private final TestHazelcastFactory factory = new TestHazelcastFactory();
    private final String testObjectName = randomString();

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Test
    public void testMapAllPermission() {
        final Config config = createConfig();
        PermissionConfig perm = addPermission(config, "dev");
        perm.addAction(ActionConstants.ACTION_ALL);

        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();
        IMap<String, String> map = client.getMap(testObjectName);
        map.put("1", "A");
        map.get("1");
        map.lock("1");
        map.unlock("1");
        map.destroy();
    }

    @Test
    public void testMapAllPermission_multiplePrincipals_withAllowedOne() {
        String loginUser = "UserA";
        String principal = "UserA,UserB";

        final Config config = createConfig(loginUser);

        PermissionConfig perm = addPermission(config, principal);
        perm.addAction(ActionConstants.ACTION_ALL);

        final ClientConfig cc = new ClientConfig();
        cc.setCredentials(new ClientCustomAuthenticationTest.CustomCredentials(loginUser, "", ""));

        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient(cc);
        IMap<String, String> map = client.getMap(testObjectName);
        map.put("1", "A");
        map.get("1");
        map.lock("1");
        map.unlock("1");
        map.destroy();
    }

    @Test
    public void testMapAllPermission_multiplePrincipals_withAllowedOne_fromXML()
            throws IOException {
        String loginUser = "role1";

        String xml = HAZELCAST_START_TAG
                + "<security enabled=\"true\">"
                + "    <client-permissions>"
                + "        <map-permission name=\"mySecureMap\" principal=\"role1,role2\">"
                + "            <endpoints>"
                + "                <endpoint>10.10.*.*</endpoint>"
                + "                <endpoint>127.0.0.1</endpoint>"
                + "            </endpoints>"
                + "            <actions>"
                + "                <action>put</action>"
                + "                <action>read</action>"
                + "                <action>destroy</action>"
                + "            </actions>"
                + "        </map-permission>"
                + "    </client-permissions>"
                + "</security>"
                + HAZELCAST_END_TAG;
        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        Config config = new XmlConfigBuilder(bis).build();
        bis.close();
        addCustomUserLoginModule(config, loginUser);

        final ClientConfig cc = new ClientConfig();
        cc.setCredentials(new ClientCustomAuthenticationTest.CustomCredentials(loginUser, "", ""));

        // Create from member
        HazelcastInstance member = factory.newHazelcastInstance(config);
        member.getMap("mySecureMap");

        // Check permissions
        HazelcastInstance client = factory.newHazelcastClient(cc);
        IMap<String, String> map = client.getMap("mySecureMap");
        map.put("1", "A");
        map.get("1");
        map.destroy();
    }

    @Test
    public void testMapAllPermission_wildcardAllPrincipal() {
        String loginUser = "UserA";
        String principal = "*";

        final Config config = createConfig(loginUser);

        PermissionConfig perm = addPermission(config, principal);
        perm.addAction(ActionConstants.ACTION_ALL);

        final ClientConfig cc = new ClientConfig();
        cc.setCredentials(new ClientCustomAuthenticationTest.CustomCredentials(loginUser, "", ""));

        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient(cc);
        IMap<String, String> map = client.getMap(testObjectName);
        map.put("1", "A");
        map.get("1");
        map.lock("1");
        map.unlock("1");
        map.destroy();
    }

    @Test
    public void testMapAllPermission_nullPrincipal() {
        String loginUser = "UserA";
        final Config config = createConfig(loginUser);

        PermissionConfig perm = addPermission(config, null);
        perm.addAction(ActionConstants.ACTION_ALL);

        final ClientConfig cc = new ClientConfig();
        cc.setCredentials(new ClientCustomAuthenticationTest.CustomCredentials(loginUser, "", ""));

        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient(cc);
        IMap<String, String> map = client.getMap(testObjectName);
        map.put("1", "A");
        map.get("1");
        map.lock("1");
        map.unlock("1");
        map.destroy();
    }

    @Test
    public void testMapAllPermission_multiplePrincipalWithoutAllowedOne_andMalformedStringEndingWithSeparator() {
        String loginUser = "UserA";
        String principal = "UserB,UserC,";

        final Config config = createConfig(loginUser);

        PermissionConfig perm = addPermission(config, principal);
        perm.addAction(ActionConstants.ACTION_ALL);

        final ClientConfig cc = new ClientConfig();
        cc.setCredentials(new ClientCustomAuthenticationTest.CustomCredentials(loginUser, "", ""));

        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient(cc);

        expectedException.expect(AccessControlException.class);
        client.getMap(testObjectName);
    }

    @Test
    public void testMapPermissionActions() {
        final Config config = createConfig();
        addPermission(config, "dev")
                .addAction(ActionConstants.ACTION_PUT)
                .addAction(ActionConstants.ACTION_READ)
                .addAction(ActionConstants.ACTION_REMOVE);

        factory.newHazelcastInstance(config).getMap(testObjectName); // create map
        HazelcastInstance client = factory.newHazelcastClient();
        IMap<String, String> map = client.getMap(testObjectName);
        assertNull(map.put("1", "A"));
        assertEquals("A", map.get("1"));
        assertEquals("A", map.remove("1"));
        expectedException.expect(RuntimeException.class);
        map.lock("1"); // throw exception
    }

    @Test
    public void testMapReadPermission_alreadyCreatedMap() {
        final Config config = createConfig();
        addPermission(config, "dev")
                .addAction(ActionConstants.ACTION_READ);
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        instance.getMap(testObjectName).put("key", "value");
        HazelcastInstance client = factory.newHazelcastClient();
        IMap<Object, Object> map = client.getMap(testObjectName);
        assertEquals("value", map.get("key"));
    }

    private Config createConfig() {
        final Config config = new Config();
        final SecurityConfig secCfg = config.getSecurityConfig();
        secCfg.setEnabled(true);
        return config;
    }

    private Config createConfig(String allowedLoginUsername) {
        final Config config = createConfig();
        addCustomUserLoginModule(config, allowedLoginUsername);
        return config;
    }

    private PermissionConfig addPermission(Config config, String principal) {
        PermissionConfig perm = new PermissionConfig(PermissionType.MAP, testObjectName, principal);
        config.getSecurityConfig().addClientPermissionConfig(perm);
        return perm;
    }

    private void addCustomUserLoginModule(Config config, String allowedLoginUsername) {
        Properties prop = new Properties();
        prop.setProperty("username", allowedLoginUsername);
        prop.setProperty("key1", "");
        prop.setProperty("key2", "");

        SecurityConfig secCfg = config.getSecurityConfig();

        secCfg.addClientLoginModuleConfig(
                new LoginModuleConfig()
                        .setUsage(LoginModuleConfig.LoginModuleUsage.REQUIRED)
                        .setClassName(ClientCustomAuthenticationTest.CustomLoginModule.class.getName())
                        .setProperties(prop));
    }

}
