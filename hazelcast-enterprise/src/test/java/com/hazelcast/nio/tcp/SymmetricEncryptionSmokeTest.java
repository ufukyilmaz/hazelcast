package com.hazelcast.nio.tcp;

import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigAccessor;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParametersRunnerFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static com.hazelcast.test.HazelcastTestSupport.assertClusterSize;
import static com.hazelcast.test.HazelcastTestSupport.ignore;
import static org.junit.Assert.fail;

/**
 * Quick symmetric encryption tests.
 */
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseParametersRunnerFactory.class)
@Category({ QuickTest.class })
public class SymmetricEncryptionSmokeTest extends AbstractSymmetricEncryptionTestBase {

    @Parameterized.Parameters(name = "advancedNetworking:{0}")
    public static Collection<Boolean> parameters() {
        return Arrays.asList(false, true);
    }

    @Parameterized.Parameter
    public boolean advancedNetworking;

    @Test
    public void testPbeJoining() {
        Config config = createPbeConfig("password", 123, advancedNetworking);
        HazelcastInstance h1 = factory.newHazelcastInstance(config);
        HazelcastInstance h2 = factory.newHazelcastInstance(config);
        assertClusterSize(2, h1, h2);
    }

    @Test
    public void testAesJoining() {
        assumeCipherSupported(CIPHER_AES);
        Config config = createPbeConfig("password", 123, advancedNetworking);
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
        Config config1 = createConfig(key, CIPHER_AES, advancedNetworking);
        Config config2 = createConfig(key, CIPHER_AES, advancedNetworking);
        ConfigAccessor.getActiveMemberNetworkConfig(config1).getSymmetricEncryptionConfig().setPassword("pass1");
        ConfigAccessor.getActiveMemberNetworkConfig(config2).getSymmetricEncryptionConfig().setPassword("pass2");
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
        Config config1 = createPbeConfig("pass", 7, advancedNetworking);
        Config config2 = createPbeConfig("pass", 7, advancedNetworking);

        byte[] key1 = generateRandomKey(16);
        byte[] key2 = Arrays.copyOf(key1, key1.length);
        // XOR the first byte to make it different
        key2[0] ^= 0xff;
        ConfigAccessor.getActiveMemberNetworkConfig(config1).getSymmetricEncryptionConfig().setKey(key1);
        ConfigAccessor.getActiveMemberNetworkConfig(config1).getSymmetricEncryptionConfig().setKey(key2);

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

        HazelcastInstance h1 = factory.newHazelcastInstance(createConfig(key1, CIPHER_AES, advancedNetworking));

        try {
            factory.newHazelcastInstance(createConfig(key2, CIPHER_AES, advancedNetworking));
            fail("Node should not be able to start.");
        } catch (IllegalStateException ex) {
            ignore(ex);
        }

        assertClusterSize(1, h1);
    }

    @Test
    public void testPbeNotJoiningIfPasswordsDiffer() {
        HazelcastInstance h1 = factory.newHazelcastInstance(createPbeConfig("pass1", 1, advancedNetworking));

        try {
            factory.newHazelcastInstance(createPbeConfig("pass2", 1, advancedNetworking));
            fail("Node should not be able to start.");
        } catch (IllegalStateException ex) {
            ignore(ex);
        }

        assertClusterSize(1, h1);
    }

    @Test
    public void testPbeNotJoiningIfIterationsDiffer() {
        HazelcastInstance h1 = factory.newHazelcastInstance(createPbeConfig("pass", 1, advancedNetworking));

        try {
            factory.newHazelcastInstance(createPbeConfig("pass", 2, advancedNetworking));
            fail("Node should not be able to start.");
        } catch (IllegalStateException ex) {
            ignore(ex);
        }

        assertClusterSize(1, h1);
    }
}
