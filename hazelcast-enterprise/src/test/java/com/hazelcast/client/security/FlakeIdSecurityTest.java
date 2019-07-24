package com.hazelcast.client.security;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.PermissionConfig;
import com.hazelcast.config.PermissionConfig.PermissionType;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
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

import static com.hazelcast.test.HazelcastTestSupport.randomString;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class FlakeIdSecurityTest {

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
    public void testFlakeIdGeneratorPermissions() {
        final Config config = createConfig();
        addPermission(config)
        .addAction(ActionConstants.ACTION_CREATE)
        .addAction(ActionConstants.ACTION_MODIFY);
        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();

        FlakeIdGenerator flakeIdGenerator = client.getFlakeIdGenerator(testObjectName);
        flakeIdGenerator.newId();
        expectedException.expect(AccessControlException.class);
        client.getFlakeIdGenerator("test2");
    }

    @Test
    public void testFlakeIdGeneratorPermissionsInXml() throws IOException {
        String xml = HAZELCAST_START_TAG
                + "<security enabled=\"true\">"
                + "    <client-permissions>"
                + "        <flake-id-generator-permission name='test'>"
                + "            <endpoints>"
                + "                <endpoint>127.0.0.1</endpoint>"
                + "            </endpoints>"
                + "            <actions>"
                + "                <action>all</action>"
                + "            </actions>"
                + "        </flake-id-generator-permission>"
                + "    </client-permissions>"
                + "</security>"
                + HAZELCAST_END_TAG;
        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        Config config = new XmlConfigBuilder(bis).build();
        bis.close();

        addPermission(config)
        .addAction(ActionConstants.ACTION_CREATE)
        .addAction(ActionConstants.ACTION_MODIFY);
        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();

        FlakeIdGenerator flakeIdGenerator = client.getFlakeIdGenerator(testObjectName);
        flakeIdGenerator.newId();
        expectedException.expect(AccessControlException.class);
        client.getFlakeIdGenerator("test2");
    }

    private Config createConfig() {
        final Config config = new Config();
        final SecurityConfig secCfg = config.getSecurityConfig();
        secCfg.setEnabled(true);
        return config;
    }

    private PermissionConfig addPermission(Config config) {
        PermissionConfig perm = new PermissionConfig(PermissionType.FLAKE_ID_GENERATOR, testObjectName, "dev");
        config.getSecurityConfig().addClientPermissionConfig(perm);
        return perm;
    }
}
