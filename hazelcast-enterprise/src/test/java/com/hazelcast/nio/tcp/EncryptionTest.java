package com.hazelcast.nio.tcp;

import com.hazelcast.config.Config;
import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * Simple symmetric encryption test with and without provided secret key.
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(NightlyTest.class)
public class EncryptionTest extends HazelcastTestSupport {

    @After
    public void tearDown() {
        HazelcastInstanceFactory.terminateAll();
    }

    @Test
    public void testSymmetricEncryption_withCalculatedKey() {
        testSymmetricEncryption(null);
    }

    @Test
    public void testSymmetricEncryption_withProvidedKey() {
        byte[] key = new byte[128];
        new Random().nextBytes(key);

        testSymmetricEncryption(key);
    }

    private void testSymmetricEncryption(byte[] key) {
        SymmetricEncryptionConfig encryptionConfig = new SymmetricEncryptionConfig()
                .setEnabled(true)
                .setAlgorithm("PBEWithMD5AndDES")
                .setKey(key);

        Config config = new Config();
        config.getNetworkConfig().setSymmetricEncryptionConfig(encryptionConfig);

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);

        assertEquals(3, h1.getCluster().getMembers().size());
        assertEquals(3, h2.getCluster().getMembers().size());
        assertEquals(3, h3.getCluster().getMembers().size());
        assertEquals(h1.getCluster().getLocalMember(), h2.getCluster().getMembers().iterator().next());
        assertEquals(h1.getCluster().getLocalMember(), h3.getCluster().getMembers().iterator().next());

        warmUpPartitions(h1, h2, h3);
        Member owner1 = h1.getPartitionService().getPartition(0).getOwner();
        Member owner2 = h2.getPartitionService().getPartition(0).getOwner();
        Member owner3 = h3.getPartitionService().getPartition(0).getOwner();
        assertEquals(owner1, owner2);
        assertEquals(owner1, owner3);

        String name = "encryption-test";
        IMap<Integer, byte[]> map1 = h1.getMap(name);
        for (int i = 1; i < 100; i++) {
            map1.put(i, new byte[1024 * i]);
        }

        IMap<Integer, byte[]> map2 = h2.getMap(name);
        for (int i = 1; i < 100; i++) {
            byte[] bytes = map2.get(i);
            assertEquals(i * 1024, bytes.length);
        }

        IMap<Integer, byte[]> map3 = h3.getMap(name);
        for (int i = 1; i < 100; i++) {
            byte[] bytes = map3.get(i);
            assertEquals(i * 1024, bytes.length);
        }
    }
}
