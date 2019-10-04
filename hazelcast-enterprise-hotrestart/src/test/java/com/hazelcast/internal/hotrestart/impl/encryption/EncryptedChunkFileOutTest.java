package com.hazelcast.internal.hotrestart.impl.encryption;

import com.hazelcast.config.EncryptionAtRestConfig;
import com.hazelcast.config.SecureStoreConfig;
import com.hazelcast.internal.hotrestart.impl.gc.MutatorCatchup;
import com.hazelcast.internal.hotrestart.impl.io.EncryptedChunkFileOut;
import com.hazelcast.internal.hotrestart.impl.testsupport.HotRestartTestUtil;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.internal.hotrestart.impl.HotRestarter.BUFFER_SIZE;
import static com.hazelcast.internal.hotrestart.impl.testsupport.HotRestartTestUtil.assertRecordEquals;
import static com.hazelcast.internal.util.BasicSymmetricCipherBuilder.findKeyAlgorithm;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EncryptedChunkFileOutTest {

    @Parameter
    public String algorithm;

    @Parameter(1)
    public int keySize;

    @Parameters(name = "algorithm:{0}, keySize:{1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {"AES", 128},
                {"AES/CBC/PKCS5Padding", 128},
                {"DESede/CBC/PKCS5Padding", 112},
        });
    }

    @Rule
    public TemporaryFolder tf = new TemporaryFolder();

    @Test
    public void testEncryptDecryptLargeValueChunk() throws Exception {
        EncryptionAtRestConfig config = new EncryptionAtRestConfig().setEnabled(true).setAlgorithm(algorithm)
                                                                    .setSecureStoreConfig(new SecureStoreConfig() {
                                                                    });

        HotRestartCipherBuilder cipherBuilder = new HotRestartCipherBuilder(config);
        byte[] key = generateKey();
        Cipher encryptCipher = cipherBuilder.create(true, key);
        File outFile = tf.newFile("e");
        EncryptedChunkFileOut out = new EncryptedChunkFileOut(outFile, mock(MutatorCatchup.class), encryptCipher);

        AtomicInteger counter = new AtomicInteger();
        HotRestartTestUtil.TestRecord rec = new HotRestartTestUtil.TestRecord(counter);
        rec.valueBytes = new byte[BUFFER_SIZE + 1];
        out.writeValueRecord(rec.recordSeq, rec.keyPrefix, rec.keyBytes, rec.valueBytes);
        out.flagForFsyncOnClose(true);
        out.close();

        Cipher decryptCipher = cipherBuilder.create(false, key);
        InputStream in = new CipherInputStream(new FileInputStream(outFile), decryptCipher);
        try (DataInputStream dataIn = new DataInputStream(in)) {
            assertRecordEquals(rec, dataIn, true);
            assertEquals(-1, dataIn.read());
        }
    }

    private byte[] generateKey() throws NoSuchAlgorithmException {
        KeyGenerator keyGenerator = KeyGenerator.getInstance(findKeyAlgorithm(algorithm));
        keyGenerator.init(keySize);
        SecretKey secretKey = keyGenerator.generateKey();
        return secretKey.getEncoded();
    }
}
