package com.hazelcast.internal.nio.tcp;

import com.hazelcast.config.Config;
import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.TestAwareInstanceFactory;
import org.junit.After;
import org.junit.AssumptionViolatedException;

import javax.crypto.Cipher;
import java.security.GeneralSecurityException;
import java.util.Random;

import static com.hazelcast.instance.EndpointQualifier.MEMBER;

/**
 * Abstract parent for symmetric encryption tests.
 */
public abstract class AbstractSymmetricEncryptionTestBase {

    protected static final String CIPHER_AES = "AES";
    protected static final String CIPHER_PBE_WITH_MD5_AND_DES = "PBEWithMD5AndDES";

    protected final TestAwareInstanceFactory factory = new TestAwareInstanceFactory();

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    /**
     * Throws {@link AssumptionViolatedException} if given {@link Cipher} algorithm name is not supported.
     */
    protected void assumeCipherSupported(String algorithm) {
        try {
            Cipher.getInstance(algorithm);
        } catch (GeneralSecurityException e) {
            throw new AssumptionViolatedException("Skipping, " + algorithm + " cipher is not supported");
        }
    }

    /**
     * Creates a configuration with PBE cipher based symmetric encryption. Default salt is used.
     */
    protected Config createPbeConfig(String password, int iterationCount, boolean advanced) {
        SymmetricEncryptionConfig encryptionConfig = new SymmetricEncryptionConfig()
                .setEnabled(true)
                .setAlgorithm(CIPHER_PBE_WITH_MD5_AND_DES)
                .setPassword(password)
                .setIterationCount(iterationCount);

        Config config = new Config();
        config.setProperty(GroupProperty.MAX_JOIN_SECONDS.getName(), "5");
        if (advanced) {
            config.getAdvancedNetworkConfig().setEnabled(true);
            config.getAdvancedNetworkConfig().getEndpointConfigs().get(MEMBER).setSymmetricEncryptionConfig(encryptionConfig);
        } else {
            config.getNetworkConfig().setSymmetricEncryptionConfig(encryptionConfig);
        }
        return config;
    }

    /**
     * Creates a configuration with key based encryption using the given algorithm. Default salt is used.
     */
    protected Config createConfig(byte[] key, String algorithm, boolean advanced) {
        SymmetricEncryptionConfig encryptionConfig = new SymmetricEncryptionConfig()
                .setEnabled(true)
                .setAlgorithm(algorithm)
                .setKey(key);

        Config config = new Config();
        config.setProperty(GroupProperty.MAX_JOIN_SECONDS.getName(), "5");
        if (advanced) {
            config.getAdvancedNetworkConfig().setEnabled(true);
            config.getAdvancedNetworkConfig().getEndpointConfigs().get(MEMBER).setSymmetricEncryptionConfig(encryptionConfig);
        } else {
            config.getNetworkConfig().setSymmetricEncryptionConfig(encryptionConfig);
        }

        return config;
    }

    /**
     * Generates new byte array of given length with random content.
     */
    protected byte[] generateRandomKey(int bytesCount) {
        byte[] key = new byte[bytesCount];
        new Random().nextBytes(key);
        return key;
    }
}
