package com.hazelcast.internal.hotrestart.impl.encryption;

import com.hazelcast.config.EncryptionAtRestConfig;
import com.hazelcast.internal.util.BasicSymmetricCipherBuilder;
import com.hazelcast.internal.util.RootCauseMatcher;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HotRestartCipherInputStreamTest {
    private static final byte[] DATA = {3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5, 8, 9, 7, 9, 3, 2, 3};
    private static final byte[] ENCRYPTION_KEY1 = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5};
    private static final byte[] ENCRYPTION_KEY2 = {9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 9, 8, 7, 6, 5, 4};

    private BasicSymmetricCipherBuilder builder;
    private byte[] encrypted;

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Before
    public void setup()
            throws BadPaddingException, IllegalBlockSizeException {
        builder = new BasicSymmetricCipherBuilder(new EncryptionAtRestConfig().setEnabled(true));
        Cipher writeCipher = builder.create(true, ENCRYPTION_KEY1);
        encrypted = writeCipher.doFinal(DATA);
        // check that we can decrypt with no problem
        Cipher readCipher = builder.create(false, ENCRYPTION_KEY1);
        readCipher.doFinal(encrypted);
    }

    @Test
    public void testFullRead()
            throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(encrypted);
        drain(new HotRestartCipherInputStream(in, builder.create(false, ENCRYPTION_KEY1)));
    }

    @Test
    public void testTruncatedRead()
            throws IOException {
        exception.expect(EOFException.class);
        byte[] truncated = Arrays.copyOf(encrypted, encrypted.length - 1);
        ByteArrayInputStream in = new ByteArrayInputStream(truncated);
        drain(new HotRestartCipherInputStream(in, builder.create(false, ENCRYPTION_KEY1)));
    }

    @Test
    public void testFullRead_whenWrongKey()
            throws IOException {
        exception.expect(IOException.class);
        // using a wrong key for decryption typically results in BadPaddingException
        exception.expectCause(new RootCauseMatcher(BadPaddingException.class));
        ByteArrayInputStream in = new ByteArrayInputStream(encrypted);
        drain(new HotRestartCipherInputStream(in, builder.create(false, ENCRYPTION_KEY2)));
    }

    private static void drain(InputStream in)
            throws IOException {
        byte[] buffer = new byte[1024];
        while (true) {
            if (in.read(buffer) == -1) {
                break;
            }
        }
    }
}
