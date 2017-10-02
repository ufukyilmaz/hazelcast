package com.hazelcast.nio.ssl;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Properties;

import static com.hazelcast.test.HazelcastTestSupport.assertClusterSize;
import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class SSLConnectionTest {

    @Before
    @After
    public void killAllHazelcastInstances() {
        HazelcastInstanceFactory.terminateAll();
    }

    @Test(timeout = 1000 * 180)
    public void testNodes() {
        Config config = new Config();
        config.setProperty(GroupProperty.IO_THREAD_COUNT.getName(), "1");
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(true).addMember("127.0.0.1").setConnectionTimeoutSeconds(3000);

        Properties props = TestKeyStoreUtil.createSslProperties();
        config.getNetworkConfig().setSSLConfig(new SSLConfig().setEnabled(true).setProperties(props));

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);

        assertClusterSize(3, h1, h2, h3);

        TestUtil.warmUpPartitions(h1, h2, h3);
        Member owner1 = h1.getPartitionService().getPartition(0).getOwner();
        Member owner2 = h2.getPartitionService().getPartition(0).getOwner();
        Member owner3 = h3.getPartitionService().getPartition(0).getOwner();
        assertEquals(owner1, owner2);
        assertEquals(owner1, owner3);

        String name = "ssl-test";
        int count = 128;
        IMap<Integer, byte[]> map1 = h1.getMap(name);
        for (int i = 1; i < count; i++) {
            map1.put(i, new byte[1024 * i]);
        }

        IMap<Integer, byte[]> map2 = h2.getMap(name);
        for (int i = 1; i < count; i++) {
            byte[] bytes = map2.get(i);
            assertEquals(i * 1024, bytes.length);
        }

        IMap<Integer, byte[]> map3 = h3.getMap(name);
        for (int i = 1; i < count; i++) {
            byte[] bytes = map3.get(i);
            assertEquals(i * 1024, bytes.length);
        }
    }

    @Test(timeout = 1000 * 600)
    public void testPutAndGetAlwaysGoesToWire() {
        Config config = new Config();
        config.setProperty(GroupProperty.IO_THREAD_COUNT.getName(), "1");
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(true).addMember("127.0.0.1").setConnectionTimeoutSeconds(3000);

        Properties props = TestKeyStoreUtil.createSslProperties();
        config.getNetworkConfig().setSSLConfig(new SSLConfig().setEnabled(true).setProperties(props));

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);

        assertClusterSize(2, h1, h2);

        TestUtil.warmUpPartitions(h1, h2);
        Member owner1 = h1.getPartitionService().getPartition(0).getOwner();
        Member owner2 = h2.getPartitionService().getPartition(0).getOwner();
        assertEquals(owner1, owner2);

        String name = "ssl-test";

        IMap<String, byte[]> map1 = h1.getMap(name);
        final int count = 256;
        for (int i = 1; i <= count; i++) {
            final String key = HazelcastTestSupport.generateKeyOwnedBy(h2);
            map1.put(key, new byte[1024 * i]);
            byte[] bytes = map1.get(key);
            assertEquals(i * 1024, bytes.length);

        }
        assertEquals(count, map1.size());
    }
}
