package com.hazelcast.nio;

import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.ByteArrayProcessor;
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

    private final SymmetricEncryptionConfig symmetricEncryptionConfig =
            new SymmetricEncryptionConfig()
                    .setEnabled(true)
                    .setPassword("foobar")
                    .setSalt("SaltedFoobar");

    @Test
    public void testEncDec() {
        ByteArrayProcessor encProcessor = new CipherByteArrayProcessor(createSymmetricWriterCipher(symmetricEncryptionConfig));
        ByteArrayProcessor decProcessor = new CipherByteArrayProcessor(createSymmetricReaderCipher(symmetricEncryptionConfig));

        String payload = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, "
                + "sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.";

        byte[] original = payload.getBytes();
        byte[] enc = encProcessor.process(original);
        byte[] dec = decProcessor.process(enc);

        assertFalse(Arrays.equals(enc, original));
        assertFalse(Arrays.equals(dec, enc));
        assertArrayEquals(dec, original);
    }

}
