package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.properties.ClientProperty;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.enterprise.SampleLicense;
import com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class ClientWitMixedClusterTest {

    private CompatibilityTestHazelcastFactory factory = new CompatibilityTestHazelcastFactory();

    @After
    public void teardown() {
        factory.terminateAll();
    }

    @Test
    public void test() {
        String currentVersion = CompatibilityTestHazelcastInstanceFactory.getCurrentVersion();
        String[] serverVersions = CompatibilityTestHazelcastInstanceFactory.getKnownReleasedAndCurrentVersions();
        for (String serverVersion : serverVersions) {
            Config config = new Config();
            config.setLicenseKey(SampleLicense.UNLIMITED_LICENSE);
            factory.newHazelcastInstance(serverVersion, config, true);
        }

        ClientConfig config = new ClientConfig();
        config.setProperty(ClientProperty.SHUFFLE_MEMBER_LIST.getName(), "false");

        HazelcastInstance hazelcastClient = factory.newHazelcastClient(currentVersion, config);

        IMap<Integer, Integer> map = hazelcastClient.getMap("test");
        for (int i = 0; i < 1000; i++) {
            map.put(i, i);
        }
        assertEquals(1000, map.size());
    }

}
