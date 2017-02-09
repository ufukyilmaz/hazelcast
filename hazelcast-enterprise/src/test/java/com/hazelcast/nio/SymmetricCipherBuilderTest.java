package com.hazelcast.nio;

import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.nio.CipherHelper.SymmetricCipherBuilder;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.util.Arrays;
import java.util.Collection;

import static com.hazelcast.nio.CipherHelper.findKeyAlgorithm;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class SymmetricCipherBuilderTest {

    @Parameter
    public String algorithm;

    @Parameter(1)
    public int keySize;

    @Parameters(name = "algorithm:{0}, keySize:{1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {"AES", 128},
                {"AES/CBC/PKCS5Padding", 128},
                {"Blowfish", 128},
                {"DESede", 112},
                {"DESede", 168},
                {"DES", 56},
                {"PBEWithMD5AndDES", 0},
        });
    }

    @Test
    public void testCreateCipher() throws Exception {
        SymmetricEncryptionConfig config = new SymmetricEncryptionConfig()
                .setEnabled(true)
                .setAlgorithm(algorithm)
                .setKey(getKey());

        Cipher cipher = new SymmetricCipherBuilder(config).create(true);

        assertEquals(algorithm, cipher.getAlgorithm());
    }

    private byte[] getKey() throws Exception {
        if (keySize == 0) {
            return null;
        }

        KeyGenerator keyGenerator = KeyGenerator.getInstance(findKeyAlgorithm(algorithm));
        keyGenerator.init(keySize);
        SecretKey secretKey = keyGenerator.generateKey();
        return secretKey.getEncoded();
    }
}
