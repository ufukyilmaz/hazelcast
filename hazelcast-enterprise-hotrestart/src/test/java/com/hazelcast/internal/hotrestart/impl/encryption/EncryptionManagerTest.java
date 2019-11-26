package com.hazelcast.internal.hotrestart.impl.encryption;

import com.hazelcast.config.EncryptionAtRestConfig;
import com.hazelcast.hotrestart.HotRestartException;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static com.hazelcast.internal.hotrestart.impl.testsupport.HotRestartTestUtil.createHotRestartLogger;
import static com.hazelcast.test.HazelcastTestSupport.assertInstanceOf;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EncryptionManagerTest {

    private static final EncryptionAtRestConfig DEFAULT_CONFIG = new EncryptionAtRestConfig().setEnabled(true);
    private static final EncryptionAtRestConfig CONFIG_WITH_KEY_SIZE = new EncryptionAtRestConfig().setEnabled(true)
                                                                                                   .setKeySize(128);
    private static final EncryptionAtRestConfig CONFIG_DISABLED = new EncryptionAtRestConfig();
    private static final byte[] MASTER_KEY_BYTES = StringUtil.stringToBytes("0123456789012345");
    private static final List<byte[]> MASTER_KEYS = Collections.singletonList(MASTER_KEY_BYTES);

    @Rule
    public TemporaryFolder tf = new TemporaryFolder();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private EncryptionManager newEncryptionManager(EncryptionAtRestConfig encryptionAtRestConfig,
                                                   InitialKeysSupplier initialKeysSupplier) {
        HotRestartCipherBuilder cipherBuilder = encryptionAtRestConfig.isEnabled() ? new HotRestartCipherBuilder(
                encryptionAtRestConfig) : null;
        HotRestartStoreEncryptionConfig config = new HotRestartStoreEncryptionConfig().setCipherBuilder(cipherBuilder)
                                                                                      .setInitialKeysSupplier(initialKeysSupplier)
                                                                                      .setKeySize(encryptionAtRestConfig
                                                                                              .getKeySize());
        return new EncryptionManager(createHotRestartLogger(), tf.getRoot(), config);
    }

    @Test
    public void testEncryptionEnabled() {
        assertTrue(newEncryptionManager(DEFAULT_CONFIG, () -> MASTER_KEYS).isEncryptionEnabled());
        assertFalse(newEncryptionManager(CONFIG_DISABLED, () -> MASTER_KEYS).isEncryptionEnabled());
    }

    @Test
    public void testInvalidConfig_whenUnsupportedAlgorithm() {
        EncryptionAtRestConfig config = new EncryptionAtRestConfig().setAlgorithm("Unsupported").setEnabled(true);
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage("Unable to create Cipher");
        newEncryptionManager(config, () -> MASTER_KEYS);
    }

    @Test
    public void testInvalidConfig_whenNoPaddingUnsupported() {
        // we don't support ciphers with no padding
        EncryptionAtRestConfig config = new EncryptionAtRestConfig().setAlgorithm("AES/CBC/NoPadding").setEnabled(true);
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage("Unable to create Cipher");
        newEncryptionManager(config, () -> MASTER_KEYS);
    }

    @Test
    public void testRetrieveEncryptionKeyException_whenKeyInvalid() {
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage("Unable to create Cipher");
        newEncryptionManager(DEFAULT_CONFIG, () -> Collections.singletonList(new byte[]{0}));
    }

    @Test
    public void testInitialize_whenNoMasterEncryptionKeyAvailable() {
        expectedException.expect(HotRestartException.class);
        expectedException.expectMessage("No master encryption key available");
        assertNoEncryptionKeyAvailable(newEncryptionManager(DEFAULT_CONFIG, Collections::emptyList));
    }

    @Test
    public void testInitialize_whenNoInitalKeysSupplier() {
        expectedException.expect(HotRestartException.class);
        expectedException.expectMessage("No master encryption key available");
        assertNoEncryptionKeyAvailable(newEncryptionManager(DEFAULT_CONFIG, null));
    }

    @Test
    public void testNewWriteCipherNull_whenEncryptionDisabled() {
        assertNull(newEncryptionManager(CONFIG_DISABLED, () -> MASTER_KEYS).newWriteCipher());
    }

    @Test
    public void testCipherInputStream() throws IOException {
        EncryptionManager encryptionMgr = newEncryptionManager(DEFAULT_CONFIG, () -> MASTER_KEYS);
        Cipher c = encryptionMgr.newWriteCipher();
        File file = createEncryptedFile(c, new byte[]{1, 2, 3});
        assertCipherInputStream(encryptionMgr, file, new byte[]{1, 2, 3});
    }

    @Test
    public void testCipherInputStreamException_whenEmptyFile() throws IOException {
        EncryptionManager encryptionMgr = newEncryptionManager(DEFAULT_CONFIG, () -> MASTER_KEYS);
        expectedException.expect(EOFException.class);
        assertCipherInputStream(encryptionMgr, tf.newFile(), new byte[0]);
    }

    @Test
    public void testWrap_whenEncryptionDisabled() {
        EncryptionManager encryptionMgr = newEncryptionManager(CONFIG_DISABLED, () -> MASTER_KEYS);
        ByteArrayInputStream in = new ByteArrayInputStream(new byte[0]);
        assertSame(in, encryptionMgr.wrap(in));
    }

    @Test
    public void testWrap_whenEncryptionEnabled() {
        EncryptionManager encryptionMgr = newEncryptionManager(DEFAULT_CONFIG, () -> MASTER_KEYS);
        assertInstanceOf(HotRestartCipherInputStream.class, encryptionMgr.wrap(new ByteArrayInputStream(new byte[0])));
    }

    @Test
    public void testKeyFile() throws IOException {
        EncryptionManager encryptionMgr = newEncryptionManager(CONFIG_WITH_KEY_SIZE, () -> MASTER_KEYS);
        byte[] key = readKeyFile(encryptionMgr, MASTER_KEY_BYTES);
        assertEquals(16, key.length);
        checkEncryptionManagerUsesTheSameKey(encryptionMgr, key);
    }

    @Test
    public void testKeyFile_whenMasterKeyRotated() throws IOException {
        EncryptionManager encryptionMgr = newEncryptionManager(CONFIG_WITH_KEY_SIZE, () -> MASTER_KEYS);
        byte[] key = readKeyFile(encryptionMgr, MASTER_KEY_BYTES);
        assertEquals(16, key.length);
        byte[] newMasterKey = StringUtil.stringToBytes("1111222233334444");
        encryptionMgr.rotateMasterKey(newMasterKey);
        byte[] newKey = readKeyFile(encryptionMgr, newMasterKey);
        assertArrayEquals(key, newKey);
        checkEncryptionManagerUsesTheSameKey(encryptionMgr, key);
    }

    @Test
    public void testExistingKeyFileReencryption_whenNewMasterKey() throws IOException {
        newEncryptionManager(CONFIG_WITH_KEY_SIZE, () -> MASTER_KEYS);
        byte[] newMasterKey = StringUtil.stringToBytes("1111222233334444");
        EncryptionManager encryptionMgr = newEncryptionManager(CONFIG_WITH_KEY_SIZE,
                () -> Arrays.asList(newMasterKey, MASTER_KEY_BYTES));
        readKeyFile(encryptionMgr, newMasterKey);
    }

    @Test
    public void testExistingKeyFile_whenMasterKeyNotFound() {
        newEncryptionManager(CONFIG_WITH_KEY_SIZE, () -> MASTER_KEYS);
        expectedException.expect(HotRestartException.class);
        expectedException.expectMessage("Cannot find master encryption key");
        newEncryptionManager(CONFIG_WITH_KEY_SIZE, () -> Collections.singletonList(StringUtil.stringToBytes("1111222233334444")));
    }

    @Test
    public void testExistingKeyFile_whenUnexpectedKeyLength() throws IOException {
        EncryptionManager encryptionMgr = newEncryptionManager(CONFIG_WITH_KEY_SIZE, () -> MASTER_KEYS);
        File keyFile = encryptionMgr.getKeyFile();
        assertTrue(keyFile.exists());
        // rewrite the key file using a shorter encryption key
        try (FileOutputStream out = new FileOutputStream(keyFile)) {
            byte[] masterKeyHash = computeHash(MASTER_KEY_BYTES);
            DataOutputStream dout = new DataOutputStream(out);
            String base64EncodedKeyHash = Base64.getEncoder().encodeToString(masterKeyHash);
            dout.writeUTF(base64EncodedKeyHash);
            dout.flush();
            Cipher cipher = encryptionMgr.createCipher(true, MASTER_KEY_BYTES);
            CipherOutputStream cout = new CipherOutputStream(out, cipher);
            byte[] key192bits = new byte[24];
            new Random().nextBytes(key192bits);
            cout.write(key192bits);
            cout.close();
        }
        expectedException.expect(HotRestartException.class);
        expectedException.expectMessage("Encryption key has length: 192, expected: 128");
        newEncryptionManager(CONFIG_WITH_KEY_SIZE, () -> MASTER_KEYS);
    }

    @Test
    public void testExistingKeyFile_whenFailedToDecrypt() throws IOException {
        EncryptionManager encryptionMgr = newEncryptionManager(CONFIG_WITH_KEY_SIZE, () -> MASTER_KEYS);
        File keyFile = encryptionMgr.getKeyFile();
        assertTrue(keyFile.exists());
        // append extra bytes to the key file to break decryption
        try (FileOutputStream out = new FileOutputStream(keyFile, true)) {
            out.write(0);
        }
        expectedException.expect(HotRestartException.class);
        expectedException.expectMessage("Failed to decrypt proxy encryption key");
        newEncryptionManager(CONFIG_WITH_KEY_SIZE, () -> MASTER_KEYS);
    }

    @Test
    public void testFailedKeyGeneration() {
        expectedException.expect(HotRestartException.class);
        expectedException.expectMessage("Unable to generate encryption key");
        newEncryptionManager(new EncryptionAtRestConfig().setEnabled(true).setKeySize(17), () -> MASTER_KEYS);
    }

    private void checkEncryptionManagerUsesTheSameKey(EncryptionManager encryptionMgr, byte[] key) throws IOException {
        Cipher writeCipher = encryptionMgr.newWriteCipher();
        File file = createEncryptedFile(writeCipher, new byte[]{1, 2, 3});
        try (CipherInputStream cin = new CipherInputStream(new FileInputStream(file), encryptionMgr.createCipher(false, key))) {
            assertInputStream(cin, new byte[]{1, 2, 3});
        }
    }

    private byte[] readKeyFile(EncryptionManager encryptionMgr, byte[] masterKey) throws IOException {
        File keyFile = encryptionMgr.getKeyFile();
        assertTrue(keyFile.exists());
        try (FileInputStream in = new FileInputStream(keyFile)) {
            DataInputStream din = new DataInputStream(in);
            String base64EncodedKeyHash = din.readUTF();
            byte[] expectedHash = Base64.getDecoder().decode(base64EncodedKeyHash);
            assertArrayEquals(expectedHash, computeHash(masterKey));
            Cipher cipher = encryptionMgr.createCipher(false, masterKey);
            CipherInputStream cin = new CipherInputStream(in, cipher);
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            IOUtil.drainTo(cin, bytes);
            return bytes.toByteArray();
        }
    }

    private File createEncryptedFile(Cipher cipher, byte[] data) throws IOException {
        File file = tf.newFile();
        try (CipherOutputStream cos = new CipherOutputStream(new FileOutputStream(file), cipher)) {
            if (data != null) {
                cos.write(data);
            }
        }
        return file;
    }

    private static void assertNoEncryptionKeyAvailable(EncryptionManager encryptionMgr) {
        try {
            encryptionMgr.newWriteCipher();
            fail("Exception expected");
        } catch (HotRestartException e) {
            // expected
        }
    }

    private static void assertCipherInputStream(EncryptionManager encryptionMgr, File file, byte[] expectedBytes)
            throws IOException {
        InputStream in = encryptionMgr.wrap(new FileInputStream(file));
        assertInputStream(in, expectedBytes);
    }

    private static void assertInputStream(InputStream in, byte[] expectedBytes) throws IOException {
        byte[] bytes = new byte[expectedBytes.length];
        IOUtil.readFully(in, bytes);
        assertArrayEquals(expectedBytes, bytes);
        assertEquals(-1, in.read());
    }

    private static byte[] computeHash(byte[] bytes) {
        byte[] hashBytes = new byte[32];
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(bytes);
            digest.digest(hashBytes, 0, hashBytes.length);
        } catch (GeneralSecurityException e) {
            throw new AssertionError(e);
        }
        return hashBytes;
    }
}
