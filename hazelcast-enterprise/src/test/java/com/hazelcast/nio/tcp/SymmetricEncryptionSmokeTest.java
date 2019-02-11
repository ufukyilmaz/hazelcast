package com.hazelcast.nio.tcp;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.annotation.QuickTest;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;

import static com.hazelcast.test.HazelcastTestSupport.assertClusterSize;

/**
 * Quick symmetric encryption tests.
 */
@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({ QuickTest.class })
@Ignore("https://github.com/hazelcast/hazelcast-enterprise/issues/2725")
public class SymmetricEncryptionSmokeTest extends AbstractSymmetricEncryptionTestBase {

    @Test
    public void testPbeJoining() {
        Config config = createPbeConfig("password", 123);
        HazelcastInstance h1 = factory.newHazelcastInstance(config);
        HazelcastInstance h2 = factory.newHazelcastInstance(config);
        assertClusterSize(2, h1, h2);
    }

    @Test
    public void testAesJoining() {
        assumeCipherSupported(CIPHER_AES);
        Config config = createPbeConfig("password", 123);
        HazelcastInstance h1 = factory.newHazelcastInstance(config);
        HazelcastInstance h2 = factory.newHazelcastInstance(config);
        assertClusterSize(2, h1, h2);
    }

    /**
     * Tests that password is not taken into account if AES encryption is used.
     */
    @Test
    public void testAesJoiningIfPasswordsDiffer() {
        assumeCipherSupported(CIPHER_AES);
        byte[] key = generateRandomKey(16);
        Config config1 = createConfig(key, CIPHER_AES);
        Config config2 = createConfig(key, CIPHER_AES);
        config1.getNetworkConfig().getSymmetricEncryptionConfig().setPassword("pass1");
        config2.getNetworkConfig().getSymmetricEncryptionConfig().setPassword("pass2");
        HazelcastInstance h1 = factory.newHazelcastInstance(config1);
        HazelcastInstance h2 = factory.newHazelcastInstance(config2);
        assertClusterSize(2, h1, h2);
    }

    /**
     * Tests that key is not taken into account if PBE encryption is used.
     */
    @Test
    public void testPbeJoiningIfKeyDiffers() {
        assumeCipherSupported(CIPHER_AES);
        Config config1 = createPbeConfig("pass", 7);
        Config config2 = createPbeConfig("pass", 7);

        byte[] key1 = generateRandomKey(16);
        byte[] key2 = Arrays.copyOf(key1, key1.length);
        // XOR the first byte to make it different
        key2[0] ^= 0xff;
        config1.getNetworkConfig().getSymmetricEncryptionConfig().setKey(key1);
        config2.getNetworkConfig().getSymmetricEncryptionConfig().setKey(key2);

        HazelcastInstance h1 = factory.newHazelcastInstance(config1);
        HazelcastInstance h2 = factory.newHazelcastInstance(config2);
        assertClusterSize(2, h1, h2);
    }

    @Test
    public void testAesNotJoiningIfKeysDiffer() {
        assumeCipherSupported(CIPHER_AES);
        byte[] key1 = generateRandomKey(16);
        byte[] key2 = Arrays.copyOf(key1, key1.length);
        // XOR the first byte to make it different
        key2[0] ^= 0xff;

        HazelcastInstance h1 = factory.newHazelcastInstance(createConfig(key1, CIPHER_AES));
        HazelcastInstance h2 = factory.newHazelcastInstance(createConfig(key2, CIPHER_AES));
        assertClusterSize(1, h1, h2);
    }

    @Test
    public void testPbeNotJoiningIfPasswordsDiffer() {
        HazelcastInstance h1 = factory.newHazelcastInstance(createPbeConfig("pass1", 1));
        HazelcastInstance h2 = factory.newHazelcastInstance(createPbeConfig("pass2", 1));

        assertClusterSize(1, h1, h2);
    }

    @Test
    public void testPbeNotJoiningIfIterationsDiffer() {
        HazelcastInstance h1 = factory.newHazelcastInstance(createPbeConfig("pass", 1));
        HazelcastInstance h2 = factory.newHazelcastInstance(createPbeConfig("pass", 2));

        assertClusterSize(1, h1, h2);
    }
}
