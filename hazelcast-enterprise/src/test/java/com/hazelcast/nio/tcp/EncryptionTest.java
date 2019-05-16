package com.hazelcast.nio.tcp;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseSerialParametersRunnerFactory;
import com.hazelcast.cluster.Member;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static com.hazelcast.test.HazelcastTestSupport.assertClusterSize;
import static com.hazelcast.test.HazelcastTestSupport.warmUpPartitions;
import static org.junit.Assert.assertEquals;

/**
 * Nightly basic symmetric encryption tests.
 */
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseSerialParametersRunnerFactory.class)
@Category(NightlyTest.class)
public class EncryptionTest extends AbstractSymmetricEncryptionTestBase {

    @Parameterized.Parameters(name = "advancedNetworking:{0}")
    public static Collection<Boolean> parameters() {
        return Arrays.asList(false, true);
    }

    @Parameterized.Parameter
    public boolean advancedNetworking;

    @Test
    public void testSymmetricEncryption_withPbe() {
        testSymmetricEncryption(createPbeConfig("secret password", 555, false));
    }

    @Test
    public void testSymmetricEncryption_withAes() {
        assumeCipherSupported(CIPHER_AES);
        // AES key has 128 bits - 16bytes
        testSymmetricEncryption(createConfig(generateRandomKey(16), CIPHER_AES, advancedNetworking));
    }

    private void testSymmetricEncryption(Config config) {
        HazelcastInstance h1 = factory.newHazelcastInstance(config);
        HazelcastInstance h2 = factory.newHazelcastInstance(config);
        HazelcastInstance h3 = factory.newHazelcastInstance(config);

        assertClusterSize(3, h1, h2, h3);

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
