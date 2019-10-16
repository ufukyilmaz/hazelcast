package com.hazelcast.spi.impl.securestore.impl;

import com.hazelcast.config.JavaKeyStoreSecureStoreConfig;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.internal.util.function.ConsumerEx;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.impl.securestore.SecureStoreException;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.hazelcast.spi.impl.securestore.impl.TestJavaKeyStoreSecureStoreUtils.KEYSTORE_PASSWORD;
import static com.hazelcast.spi.impl.securestore.impl.TestJavaKeyStoreSecureStoreUtils.KEY_BYTES;
import static com.hazelcast.spi.impl.securestore.impl.TestJavaKeyStoreSecureStoreUtils.createJavaKeyStore;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.isA;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
@SuppressWarnings("restriction")
public class JavaKeyStoreSecureStoreTest extends HazelcastTestSupport {

    @Parameters(name = "keyStoreType:{0}")
    public static Collection<Object> parameters() {
        return asList(new Object[]{"PKCS12", "JCEKS"});
    }

    @Parameter
    public String keyStoreType;

    @Rule
    public TemporaryFolder tf = new TemporaryFolder();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private static final ILogger LOGGER = Logger.getLogger(JavaKeyStoreSecureStoreTest.class);

    private static final String TEST_CERTIFICATE = "-----BEGIN CERTIFICATE-----\nMIICFTCCAX4CCQCYR5TZYCjDYjANBgkqhkiG9w\n"
            + "0BAQ0FADBPMQswCQYDVQQGEwJDWjENMAsGA1UECgwEdGVzdDENMAsGA1UECwwEdG\n"
            + "VzdDENMAsGA1UEAwwEdGVzdDETMBEGCSqGSIb3DQEJARYEdGVzdDAeFw0xOTA3Mz\n"
            + "AxMjI4NTVaFw0yOTA3MjcxMjI4NTVaME8xCzAJBgNVBAYTAkNaMQ0wCwYDVQQKDA\n"
            + "R0ZXN0MQ0wCwYDVQQLDAR0ZXN0MQ0wCwYDVQQDDAR0ZXN0MRMwEQYJKoZIhvcNAQ\n"
            + "kBFgR0ZXN0MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDQ6v4ZyDRAOAfRp1\n"
            + "5sXcGlNE4s+bpZf1mJzbvg8/xF82t33hF9FyT8KcR1JpJoHOxGrMa1avqFzd48QJ\n"
            + "lqDlrsuoaeQARW9YxKXYfI1wqSqsYKeZC8ohqr9YHEErwb6KJieF6NmjLPqIgEfZ\n"
            + "9Yo6r9EU48+JHG/Ao7hZGus8HMfwIDAQABMA0GCSqGSIb3DQEBDQUAA4GBAC+5bN\n"
            + "qnKgOmIXCJXvQMSoF4KEsyY2imL06R34wuLT5dZQYikV87SYV3AQPk+8wz42++Js\n"
            + "6AlXxqnGQPY2y8Lv+YqdESqoT40lsA65eB0HmOKWsgs1XC853DkJ1s2a6AvH0zhh\n"
            + "xcAiyLovhciZ5GsNHvNTDGFyETPSNfRb4xwLii\n-----END CERTIFICATE-----";

    private static final byte[] ANOTHER_KEY_BYTES = StringUtil.stringToBytes("1111222233334444");

    private Node getNode() {
        return getNode(createHazelcastInstance());
    }


    @Test
    public void testNonExistentPath() {
        JavaKeyStoreSecureStore store = new JavaKeyStoreSecureStore(
                new JavaKeyStoreSecureStoreConfig(new File("not-there")).setType(keyStoreType), getNode());
        expectedException.expect(SecureStoreException.class);
        expectedException.expectCause(isA(FileNotFoundException.class));
        store.retrieveEncryptionKeys();
    }

    @Test
    public void testUnsupportedType() throws IOException {
        JavaKeyStoreSecureStore store = new JavaKeyStoreSecureStore(
                new JavaKeyStoreSecureStoreConfig(tf.newFile()).setType("unsupported"), getNode());
        expectedException.expect(SecureStoreException.class);
        expectedException.expectMessage("Failed to load Java KeyStore");
        expectedException.expectCause(isA(KeyStoreException.class));
        store.retrieveEncryptionKeys();
    }

    @Test
    public void testRetrieveEncryptionKeys() throws IOException {
        JavaKeyStoreSecureStoreConfig config = createJavaKeyStore(tf.newFile(), keyStoreType, KEYSTORE_PASSWORD,
                ANOTHER_KEY_BYTES, KEY_BYTES);
        JavaKeyStoreSecureStore store = new JavaKeyStoreSecureStore(config, getNode());
        List<byte[]> keys = store.retrieveEncryptionKeys();
        assertNotNull(keys);
        assertEquals(2, keys.size());
        assertArrayEquals(KEY_BYTES, keys.get(0));
        assertArrayEquals(ANOTHER_KEY_BYTES, keys.get(1));
    }

    @Test
    public void testRetrieveEncryptionKeys_whenNoEncryptionKeys() throws IOException {
        JavaKeyStoreSecureStoreConfig config = createJavaKeyStore(tf.newFile(), keyStoreType, KEYSTORE_PASSWORD);
        JavaKeyStoreSecureStore store = new JavaKeyStoreSecureStore(config, getNode());
        List<byte[]> keys = store.retrieveEncryptionKeys();
        assertNotNull(keys);
        assertTrue(keys.isEmpty());
    }

    @Test
    public void testRetrieveEncryptionKeys_whenNoEncryptionKeys_withAlias() throws IOException {
        JavaKeyStoreSecureStoreConfig config = createJavaKeyStore(tf.newFile(), keyStoreType, KEYSTORE_PASSWORD);
        config.setCurrentKeyAlias("current");
        JavaKeyStoreSecureStore store = new JavaKeyStoreSecureStore(config, getNode());
        List<byte[]> keys = store.retrieveEncryptionKeys();
        assertNotNull(keys);
        assertTrue(keys.isEmpty());
    }

    @Test
    public void testRetrieveEncryptionKeys_whenAliasNotFound() throws IOException {
        JavaKeyStoreSecureStoreConfig config = createJavaKeyStore(tf.newFile(), keyStoreType, KEYSTORE_PASSWORD, KEY_BYTES);
        config.setCurrentKeyAlias("current");
        JavaKeyStoreSecureStore store = new JavaKeyStoreSecureStore(config, getNode());
        expectedException.expect(SecureStoreException.class);
        store.retrieveEncryptionKeys();
    }

    @Test
    public void testRetrieveEncryptionKeysAliasComesFirst() throws IOException, GeneralSecurityException {
        JavaKeyStoreSecureStoreConfig config = createJavaKeyStore(tf.newFile(), keyStoreType, KEYSTORE_PASSWORD);
        withKeyStore(config, ks -> {
            String[] aliases = new String[]{"bcd", "def", "current", "abc"};
            KeyStore.ProtectionParameter entryPassword = new KeyStore.PasswordProtection(config.getPassword().toCharArray());
            byte i = 0;
            for (String alias : aliases) {
                byte[] key = simpleKey(i++);
                SecretKey secretKey = new SecretKeySpec(key, "AES");
                KeyStore.SecretKeyEntry secret = new KeyStore.SecretKeyEntry(secretKey);
                ks.setEntry(alias, secret, entryPassword);
            }
        });
        config.setCurrentKeyAlias("current");
        JavaKeyStoreSecureStore store = new JavaKeyStoreSecureStore(config, getNode());
        List<byte[]> keys = store.retrieveEncryptionKeys();
        assertNotNull(keys);
        assertEquals(4, keys.size());
        assertArrayEquals(simpleKey((byte) 2), keys.get(0));
    }

    private static byte[] simpleKey(byte i) {
        byte[] key = new byte[16];
        key[0] = i;
        return key;
    }

    @Test
    public void testRetrieveEncryptionKeys_whenKeyWrongPassword() throws IOException, GeneralSecurityException {
        JavaKeyStoreSecureStoreConfig config = createJavaKeyStore(tf.newFile(), keyStoreType, KEYSTORE_PASSWORD);
        withKeyStore(config, ks -> {
            SecretKey secretKey = new SecretKeySpec(KEY_BYTES, "AES");
            KeyStore.SecretKeyEntry secret = new KeyStore.SecretKeyEntry(secretKey);
            KeyStore.ProtectionParameter entryPassword = new KeyStore.PasswordProtection("other-password".toCharArray());
            ks.setEntry("a", secret, entryPassword);
        });
        JavaKeyStoreSecureStore store = new JavaKeyStoreSecureStore(config, getNode());
        expectedException.expect(SecureStoreException.class);
        store.retrieveEncryptionKeys();
    }

    @Test
    public void testRetrieveEncryptionKeys_whenCertificateEntry() throws IOException, GeneralSecurityException {
        JavaKeyStoreSecureStoreConfig config = createJavaKeyStore(tf.newFile(), keyStoreType, KEYSTORE_PASSWORD, KEY_BYTES);
        withKeyStore(config, ks -> {
            Certificate cert = CertificateFactory.getInstance("X.509")
                    .generateCertificate(new ByteArrayInputStream(TEST_CERTIFICATE.getBytes()));
            ks.setCertificateEntry("certificate-entry", cert);
        });
        JavaKeyStoreSecureStore store = new JavaKeyStoreSecureStore(config, getNode());
        List<byte[]> keys = store.retrieveEncryptionKeys();
        assertNotNull(keys);
        assertEquals(1, keys.size());
        assertArrayEquals(KEY_BYTES, keys.get(0));
    }

    @Test
    public void testRetrieveEncryptionKeys_whenPrivateKey() throws IOException, GeneralSecurityException {
        JavaKeyStoreSecureStoreConfig config = createJavaKeyStore(tf.newFile(), keyStoreType, KEYSTORE_PASSWORD, KEY_BYTES);
        withKeyStore(config, ks -> {
            KeyPairGenerator keyPairGen = KeyPairGenerator.getInstance("RSA");
            keyPairGen.initialize(1024);
            KeyPair pair = keyPairGen.generateKeyPair();
            PrivateKey privateKey = pair.getPrivate();
            Certificate cert = CertificateFactory.getInstance("X.509")
                    .generateCertificate(new ByteArrayInputStream(TEST_CERTIFICATE.getBytes()));
            ks.setKeyEntry("private-key", privateKey, KEYSTORE_PASSWORD.toCharArray(), new Certificate[]{cert});
        });
        JavaKeyStoreSecureStore store = new JavaKeyStoreSecureStore(config, getNode());
        List<byte[]> keys = store.retrieveEncryptionKeys();
        assertNotNull(keys);
        assertEquals(1, keys.size());
        assertArrayEquals(KEY_BYTES, keys.get(0));
    }

    @Test
    public void testRetrieveEncryptionKeys_whenIncorrectKeyStorePassword() throws IOException {
        JavaKeyStoreSecureStoreConfig config = createJavaKeyStore(tf.newFile(), keyStoreType, KEYSTORE_PASSWORD, new byte[]{0})
                .setPassword("password-incorrect");
        JavaKeyStoreSecureStore store = new JavaKeyStoreSecureStore(config, getNode());
        expectedException.expect(SecureStoreException.class);
        expectedException.expectMessage("Failed to load Java KeyStore");
        store.retrieveEncryptionKeys();
    }

    @Test
    public void testWatchChanges() throws IOException, InterruptedException {
        byte[] KEY0 = StringUtil.stringToBytes("0000000000000000");
        byte[] KEY1 = StringUtil.stringToBytes("1000000000000000");
        byte[] KEY2 = StringUtil.stringToBytes("2000000000000000");
        byte[] KEY3 = StringUtil.stringToBytes("3000000000000000");
        File keystoreFile = tf.newFile();
        JavaKeyStoreSecureStoreConfig config = createJavaKeyStore(keystoreFile, keyStoreType, KEYSTORE_PASSWORD, KEY0,
                KEY1);
        config.setPollingInterval(2);

        Node node = getNode();

        assertJavaKeyStoreChange(() -> new JavaKeyStoreSecureStore(config, node),
                () -> createJavaKeyStore(keystoreFile, keyStoreType, KEYSTORE_PASSWORD, KEY2, KEY3),
                KEY3);
        assertJavaKeyStoreChange(() -> new JavaKeyStoreSecureStore(config, node),
                () -> createJavaKeyStore(keystoreFile, keyStoreType, KEYSTORE_PASSWORD, KEY1),
                KEY1);
    }

    private static void assertJavaKeyStoreChange(Supplier<JavaKeyStoreSecureStore> storeFn, Runnable updateKeyStoreFn,
                                                 byte[] expectedKey) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(2);
        AtomicReference<byte[]> newKey = new AtomicReference<>();
        JavaKeyStoreSecureStore store = storeFn.get();
        try {
            LOGGER.info("Started watching...");
            store.addEncryptionKeyListener(k -> {
                newKey.set(k);
                latch.countDown();
                LOGGER.info("Listener 1 received keys...");
            });
            store.addEncryptionKeyListener(k -> {
                latch.countDown();
                LOGGER.info("Listener 2 received keys...");
            });
            updateKeyStoreFn.run();
            LOGGER.info("Updated file...");
            assertTrue(latch.await(20, TimeUnit.SECONDS));
            assertNotNull(newKey.get());
            assertArrayEquals(expectedKey, newKey.get());
        } finally {
            store.dispose();
            LOGGER.info("Stopped watching...");
        }
    }

    private static void withKeyStore(JavaKeyStoreSecureStoreConfig config, ConsumerEx<KeyStore> ksAction)
            throws IOException, GeneralSecurityException {
        KeyStore ks = KeyStore.getInstance(config.getType());
        try (FileInputStream in = new FileInputStream(config.getPath())) {
            ks.load(in, config.getPassword().toCharArray());
        }
        ksAction.accept(ks);
        try (FileOutputStream out = new FileOutputStream(config.getPath())) {
            ks.store(out, config.getPassword().toCharArray());
        }
    }
}
