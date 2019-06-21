package com.hazelcast.test.modulepath;

import static com.hazelcast.test.modulepath.EnterpriseTestUtils.assertClusterSize;
import static com.hazelcast.test.modulepath.EnterpriseTestUtils.createConfigWithTcpJoin;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Set;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Test;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.Config;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;

/**
 * Basic test which checks if correct Hazelcast modules are on the modulepath. It also checks that Hazelcast members and clients
 * are able to start and form a cluster.
 */
public class SmokeEnterpriseModulePathTest {

    @After
    public void after() {
        HazelcastClient.shutdownAll();
        HazelcastInstanceFactory.terminateAll();
    }

    /**
     * Verify the Hazelcast JARs are on the modulepath.
     */
    @Test
    public void testModulePath() {
        String modulePath = System.getProperty("jdk.module.path");
        assertNotNull("Module path was expected", modulePath);
        assertTrue("Module path should contain hazelcast-enterprise JAR",
                modulePath.matches(".*hazelcast-enterprise-[1-9][\\p{Alnum}\\-_\\.]+\\.jar.*"));
        assertTrue("Module path should contain hazelcast-enterprise-client JAR",
                modulePath.matches(".*hazelcast-enterprise-client-[\\p{Alnum}\\-_\\.]+\\.jar.*"));
        assertFalse("Module path must not contain hazelcast (OS) JAR",
                modulePath.matches(".*hazelcast-[0-9][\\p{Alnum}\\-_\\.]+\\.jar.*"));
        assertFalse("Module path must not contain hazelcast-client (OS) JAR",
                modulePath.matches(".*hazelcast-client-[\\p{Alnum}\\-_\\.]+\\.jar.*"));
    }

    /**
     * Verify the Hazelcast modules with correct names are used.
     */
    @Test
    public void testModuleNames() {
        Set<String> hazelcastModuleNames = ModuleLayer.boot().modules().stream().map(Module::getName)
                .filter(s -> s.contains("hazelcast")).collect(Collectors.toSet());
        assertThat(hazelcastModuleNames, hasItems("com.hazelcast.core", "com.hazelcast.client", "com.hazelcast.tests"));
    }

    /**
     * Verify Hazelcast members are able to start and form cluster. It also verifies the client is able to join and work with
     * the cluster.
     */
    @Test
    public void testCluster() {
        Config config = new Config();
        config.setLicenseKey("UNLIMITED_LICENSE#99Nodes#VuE0OIH7TbfKwAUNmSj1JlyFkr6a53911000199920009119011112151009");

        NetworkConfig networkConfig = config.getNetworkConfig();
        networkConfig.getInterfaces().addInterface("127.0.0.1");
        networkConfig.getJoin().getMulticastConfig().setEnabled(false);
        networkConfig.getJoin().getTcpIpConfig().setEnabled(true).addMember("127.0.0.1");

        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(createConfigWithTcpJoin(5701));
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(createConfigWithTcpJoin(5702));
        hz1.getMap("test").put("a", "b");
        assertClusterSize(2, hz1, hz2);
        HazelcastInstance client = HazelcastClient.newHazelcastClient();
        assertEquals("b", client.getMap("test").get("a"));
    }
}
