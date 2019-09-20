package com.hazelcast.nio;

import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.util.ByteArrayProcessor;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;

import static com.hazelcast.nio.CipherHelper.createSymmetricReaderCipher;
import static com.hazelcast.nio.CipherHelper.createSymmetricWriterCipher;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category(QuickTest.class)
public class CipherByteArrayProcessorTest {

    private static final String PAYLOAD = "Lorem ipsum dolor sit amet, consectetur adipiscing elit,"
            + " sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.";

    @Test
    public void testEncDec() {
        SymmetricEncryptionConfig config = new SymmetricEncryptionConfig()
                .setEnabled(true)
                .setPassword("foobar")
                .setSalt("SaltedFoobar");

        ByteArrayProcessor encProcessor = new CipherByteArrayProcessor(createSymmetricWriterCipher(config));
        ByteArrayProcessor decProcessor = new CipherByteArrayProcessor(createSymmetricReaderCipher(config));

        byte[] original = PAYLOAD.getBytes();
        byte[] enc = encProcessor.process(original);
        byte[] dec = decProcessor.process(enc);

        assertFalse("The encoded bytes should not be the same as the original bytes", Arrays.equals(enc, original));
        assertFalse("The decoded bytes should not be the same as the encoded bytes", Arrays.equals(dec, enc));
        assertArrayEquals("The decoded bytes should be the same as the original bytes", dec, original);
    }
}
