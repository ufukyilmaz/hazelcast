package com.hazelcast.client.map;

import com.hazelcast.client.CompatibilityTestHazelcastFactory;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

// RU_COMPAT_3_10
@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({CompatibilityTest.class})
public class ClientMapMultiVersionUnsupportedOpsTest extends HazelcastTestSupport {

    private CompatibilityTestHazelcastFactory factory = new CompatibilityTestHazelcastFactory();

    @After
    public void teardown() {
        factory.terminateAll();
    }

    private ClientConfig newClientConfigPointingToNode(HazelcastInstance instance) {
        InetSocketAddress hzAddress = (InetSocketAddress) instance.getLocalEndpoint().getSocketAddress();

        ClientConfig config = new ClientConfig();
        config.setNetworkConfig(
                new ClientNetworkConfig()
                        .setSmartRouting(false)
                        .addAddress(hzAddress.getHostName() + ":" + hzAddress.getPort()));
        return config;
    }

    @Test
    public void put_shouldBeAllowedWithoutMaxIdle()
            throws ExecutionException, InterruptedException {
        // 3.10.5
        factory.newHazelcastInstance("3.10.5", new Config(), true);
        factory.newHazelcastInstance("3.10.5", new Config(), true);
        // 3.11
        factory.newHazelcastInstance(new Config());
        HazelcastInstance hz311 = factory.newHazelcastInstance(new Config());
        HazelcastInstance instance = factory.newHazelcastClient(newClientConfigPointingToNode(hz311));

        IMap<String, String> map = instance.getMap("Test");
        map.put("One", "Two");
        map.put("Two", "Three", 1, TimeUnit.MILLISECONDS);

        map.putAsync("Three", "Four").get();
        map.putAsync("Four", "Five", 1, TimeUnit.MILLISECONDS).get();

        map.putIfAbsent("Five", "Six");
        map.putIfAbsent("Six", "Seven", 1, TimeUnit.MILLISECONDS);

        map.putTransient("Seven", "Eight", 1, TimeUnit.MILLISECONDS);

        map.set("One", "Twoo");
        map.set("Two", "Threee", 1, TimeUnit.MILLISECONDS);

        map.setAsync("One", "Twooo").get();
        map.setAsync("Two", "Threeee", 1, TimeUnit.MILLISECONDS).get();
    }


    @Test
    public void put_shouldNotBeAllowedWithMaxIdleMultiVersionOnOldMember() {
        // 3.10.5
        factory.newHazelcastInstance("3.10.5", new Config(), true);
        HazelcastInstance hz310 = factory.newHazelcastInstance("3.10.5", new Config(), true);
        // 3.11
        factory.newHazelcastInstance(new Config());
        HazelcastInstance hz311 = factory.newHazelcastInstance(new Config());

        HazelcastInstance instance = factory.newHazelcastClient(newClientConfigPointingToNode(hz310));
        IMap<String, String> map = instance.getMap("Test");

        assertMaxIdleNotAllowed(map, "Unrecognized client message received with type");
    }

    @Test
    public void put_shouldNotBeAllowedWithMaxIdleMultiVersionOnNewMember() {
        factory.newHazelcastInstance("3.10.3", new Config(), true);

        // 3.11
        factory.newHazelcastInstance(new Config());
        HazelcastInstance hz311 = factory.newHazelcastInstance(new Config());

        HazelcastInstance instance = factory.newHazelcastClient(newClientConfigPointingToNode(hz311));
        IMap<String, String> map = instance.getMap("Test");

        assertMaxIdleNotAllowed(map, "Setting MaxIdle is available when cluster version is 3.11 or higher");
    }

    private void assertMaxIdleNotAllowed(IMap<String, String> map, String expectedErrorMsg) {
        try {
            map.put("Two", "Three", 1, TimeUnit.MILLISECONDS, 1, TimeUnit.MILLISECONDS);
            fail("Put with max-idle succeeded");
        } catch (Exception ex) {
            assertContains(ex.getMessage(), expectedErrorMsg);
        }

        try {
            map.putAsync("Four", "Five", 1, TimeUnit.MILLISECONDS, 1, TimeUnit.MILLISECONDS)
               .get();
            fail("Put with max-idle succeeded");
        } catch (Exception ex) {
            assertContains(ex.getMessage(), expectedErrorMsg);
        }

        try {
            map.putIfAbsent("Six", "Seven", 1, TimeUnit.MILLISECONDS, 1, TimeUnit.MILLISECONDS);
            fail("Put with max-idle succeeded");
        } catch (Exception ex) {
            assertContains(ex.getMessage(), expectedErrorMsg);
        }

        try {
            map.putTransient("Seven", "Eight", 1, TimeUnit.MILLISECONDS, 1, TimeUnit.MILLISECONDS);
            fail("Put with max-idle succeeded");
        } catch (Exception ex) {
            assertContains(ex.getMessage(), expectedErrorMsg);
        }

        try {
            map.set("Two", "Three", 1, TimeUnit.MILLISECONDS, 1, TimeUnit.MILLISECONDS);
            fail("Put with max-idle succeeded");
        } catch (Exception ex) {
            assertContains(ex.getMessage(), expectedErrorMsg);
        }

        try {
            map.setAsync("Four", "Five", 1, TimeUnit.MILLISECONDS, 1, TimeUnit.MILLISECONDS)
               .get();
            fail("Put with max-idle succeeded");
        } catch (Exception ex) {
            assertContains(ex.getMessage(), expectedErrorMsg);
        }
    }

    @Test
    public void put_shouldBeAllowedFromAnOlderClientTo311Cluster()
            throws ExecutionException, InterruptedException  {
        factory.newInstances(new Config(), 3);

        HazelcastInstance instance = factory.newHazelcastClient("3.10", new ClientConfig());

        IMap<String, String> map = instance.getMap("Test");
        map.put("One", "Two");
        map.put("Two", "Three", 1, TimeUnit.MINUTES);

        map.putAsync("Three", "Four").get();
        map.putAsync("Four", "Five", 1, TimeUnit.MINUTES).get();

        map.putIfAbsent("Five", "Six");
        map.putIfAbsent("Six", "Seven", 1, TimeUnit.MINUTES);

        map.putTransient("Seven", "Eight", 1, TimeUnit.MINUTES);

        map.set("One", "Twoo");
        map.set("Two", "Threee", 1, TimeUnit.MINUTES);

        map.setAsync("One", "Twooo").get();
        map.setAsync("Two", "Threeee", 1, TimeUnit.MINUTES).get();

        assertEquals(7, map.size());
    }
}
