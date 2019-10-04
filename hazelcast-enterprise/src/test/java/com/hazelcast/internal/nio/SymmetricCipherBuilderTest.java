package com.hazelcast.internal.nio;

import com.hazelcast.config.AbstractSymmetricEncryptionConfig;
import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.internal.util.BasicSymmetricCipherBuilder;
import com.hazelcast.internal.util.BasicSymmetricCipherBuilderTest;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import javax.crypto.Cipher;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SymmetricCipherBuilderTest extends BasicSymmetricCipherBuilderTest {

    @Parameter(2)
    public String password;

    @Parameter(3)
    public int iterationCount;

    @Parameters(name = "algorithm:{0}, keySize:{1}, password:{2}, iterationCount:{3}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {"AES", 128, null, 0},
                {"AES/CBC/PKCS5Padding", 128, null, 0},
                {"Blowfish", 128, null, 0},
                {"DESede", 112, null, 0},
                {"DESede", 168, null, 0},
                {"DES", 56, null, 0},
                {"PBEWithMD5AndDES", 0, "password", 10},
                {"PBEWithMD5AndDES", 0, null, 10},
        });
    }

    @Test
    @Override
    public void testEncryptDecrypt_whenNullKey() throws Exception {
        // null key triggers key auto-generation
        AbstractSymmetricEncryptionConfig config = config();
        byte[] data = new byte[16 * 1024];
        new Random().nextBytes(data);
        Cipher encryptCipher = createCipher(config, true, null);
        Cipher decryptCipher = createCipher(config, false, null);
        assertArrayEquals(data, decryptCipher.doFinal(encryptCipher.doFinal(data)));
    }

    @Override
    protected BasicSymmetricCipherBuilder builder(AbstractSymmetricEncryptionConfig<?> config) {
        return new CipherHelper.SymmetricCipherBuilder((SymmetricEncryptionConfig) config);
    }

    @Override
    protected SymmetricEncryptionConfig config() {
        return new SymmetricEncryptionConfig()
                .setEnabled(true)
                .setAlgorithm(algorithm)
                .setPassword(password)
                .setIterationCount(iterationCount);
    }

    @Override
    protected AbstractSymmetricEncryptionConfig<?> invalidConfig() {
        return new SymmetricEncryptionConfig() { }
                .setEnabled(true)
                .setAlgorithm("invalidAlgorithm");
    }

}
