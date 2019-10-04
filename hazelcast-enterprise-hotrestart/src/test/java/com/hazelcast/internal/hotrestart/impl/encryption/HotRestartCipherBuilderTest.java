package com.hazelcast.internal.hotrestart.impl.encryption;

import com.hazelcast.config.AbstractSymmetricEncryptionConfig;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.security.InvalidParameterException;
import java.security.NoSuchAlgorithmException;

import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HotRestartCipherBuilderTest {

    private enum TestCipher {
        AES_128("AES", 128),
        AES_CBC_NoPadding_128("AES/CBC/NoPadding", 128),
        AES_CBC_PKCS5Padding_128("AES/CBC/PKCS5Padding", 128),
        AES_CBC_PKCS5Padding_192("AES/CBC/PKCS5Padding", 192),
        AES_CBC_PKCS5Padding_256("AES/CBC/PKCS5Padding", 256),
        AES_ECB_NoPadding_128("AES/ECB/NoPadding", 128),
        AES_ECB_PKCS5Padding_128("AES/ECB/PKCS5Padding", 128),
        Blowfish_128("Blowfish", 128),
        DESede_112("DESede", 112),
        DESede_CBC_PKCS5Padding_112("DESede/CBC/PKCS5Padding", 112),
        DESede_CBC_NoPadding_112("DESede/CBC/NoPadding", 112),
        DESede_168("DESede", 168),
        DESede_CBC_PKCS5Padding_168("DESede/CBC/PKCS5Padding", 168),
        DESede_CBC_NoPadding_168("DESede/CBC/NoPadding", 168),
        DES_56("DES", 56);

        private final String algorithm;
        private final int keySize;

        TestCipher(String algorithm, int keySize) {
            this.algorithm = algorithm;
            this.keySize = keySize;
        }

    }

    private static AbstractSymmetricEncryptionConfig<?> config(String algorithm) {
        return new AbstractSymmetricEncryptionConfig<AbstractSymmetricEncryptionConfig>() {
        }.setEnabled(true).setAlgorithm(algorithm);
    }

    @Test
    public void testGenerateKey() {
        for (TestCipher tc : TestCipher.values()) {
            AbstractSymmetricEncryptionConfig<?> config = config(tc.algorithm);
            try {
                new HotRestartCipherBuilder(config).generateKey(tc.keySize);
                // try also the "default" key size
                new HotRestartCipherBuilder(config).generateKey(0);
            } catch (Exception e) {
                e.printStackTrace();
                fail(tc.toString());
            }
        }
    }

    @Test(expected = NoSuchAlgorithmException.class)
    public void testGenerateKey_whenUnsupportedAlgorithm() throws NoSuchAlgorithmException {
        AbstractSymmetricEncryptionConfig<?> config = config("unsupported");
        new HotRestartCipherBuilder(config).generateKey(0);
    }

    @Test(expected = InvalidParameterException.class)
    public void testGenerateKey_whenUnsupportedKeySize() throws NoSuchAlgorithmException {
        AbstractSymmetricEncryptionConfig<?> config = config("AES/CBC/PKCS5Padding");
        new HotRestartCipherBuilder(config).generateKey(179);
    }
}
