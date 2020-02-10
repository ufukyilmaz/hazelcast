package com.hazelcast.spi.impl.securestore.impl;

import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.VaultSecureStoreConfig;
import com.hazelcast.enterprise.EnterpriseSerialParametersRunnerFactory;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.impl.securestore.SecureStoreException;
import com.hazelcast.test.Accessors;
import com.hazelcast.test.HazelcastTestSupport;
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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.hazelcast.spi.impl.securestore.impl.MockVaultServer.start;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeThat;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseSerialParametersRunnerFactory.class)
@Category({QuickTest.class})
public class VaultSecureStoreTest extends HazelcastTestSupport {

    private static final ILogger LOGGER = Logger.getLogger(VaultSecureStoreTest.class);

    @Parameter
    public boolean https;

    @Parameters(name = "https:{0}")
    public static Collection<Object> parameters() {
        return Arrays.asList(new Object[]{false, true});
    }

    @Rule
    public TemporaryFolder tf = new TemporaryFolder();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private Node getNode() {
        return Accessors.getNode(createHazelcastInstance());
    }

    @Test
    public void testInvalidConfiguration_whenNoAddress() {
        expectedException.expect(NullPointerException.class);
        expectedException.expectMessage(containsString("Vault server address cannot be null!"));
        new VaultSecureStoreConfig(null, "foo", "bar");
    }

    @Test
    public void testInvalidConfiguration_whenNoSecretPath() {
        expectedException.expect(NullPointerException.class);
        expectedException.expectMessage(containsString("Vault secret path cannot be null!"));
        new VaultSecureStoreConfig("http://foo", null, "bar");
    }

    @Test
    public void testInvalidConfiguration_whenNoToken() {
        expectedException.expect(NullPointerException.class);
        expectedException.expectMessage(containsString("Vault token cannot be null!"));
        new VaultSecureStoreConfig("http://foo", "bar", null);
    }

    @Test
    public void testInvalidConfiguration_unsupportedProtocol() {
        VaultSecureStore store = new VaultSecureStore(new VaultSecureStoreConfig("foo://path", "bar", "baz"),
                getNode());
        expectedException.expect(SecureStoreException.class);
        expectedException.expectMessage(containsString("Failed to retrieve encryption keys"));
        expectedException.expectCause(hasMessage(containsString("unknown protocol: foo")));
        store.retrieveEncryptionKeys();
    }

    @Test
    public void testNoKeyFound() throws Exception {
        doTestWithServer(vaultSecureStoreConfig -> {
            vaultSecureStoreConfig.setSecretPath(MockVaultServer.SECRET_PATH_WITHOUT_SECRET).setToken(MockVaultServer.TOKEN);
            VaultSecureStore store = new VaultSecureStore(vaultSecureStoreConfig, getNode());
            List<byte[]> keys = store.retrieveEncryptionKeys();
            assertNotNull(keys);
            assertTrue(keys.isEmpty());
        });
    }

    @Test
    public void testSecretInvalidBase64() throws Exception {
        doTestWithServer(vaultSecureStoreConfig -> {
            vaultSecureStoreConfig.setSecretPath(MockVaultServer.SECRET_PATH_INVALID_BASE64).setToken(MockVaultServer.TOKEN);
            VaultSecureStore store = new VaultSecureStore(vaultSecureStoreConfig, getNode());
            expectedException.expect(SecureStoreException.class);
            expectedException.expectMessage(containsString("Failed to retrieve encryption keys"));
            expectedException.expectCause(
                    allOf(isA(SecureStoreException.class), hasMessage(containsString("Failed to Base64-decode encryption key"))));
            store.retrieveEncryptionKeys();
        });
    }

    @Test
    public void testMultipleSecrets() throws Exception {
        doTestWithServer(vaultSecureStoreConfig -> {
            vaultSecureStoreConfig.setSecretPath(MockVaultServer.SECRET_PATH_MULTIPLE_SECRETS).setToken(MockVaultServer.TOKEN);
            VaultSecureStore store = new VaultSecureStore(vaultSecureStoreConfig, getNode());
            expectedException.expect(SecureStoreException.class);
            expectedException.expectMessage(containsString("Failed to retrieve encryption keys"));
            expectedException.expectCause(allOf(isA(SecureStoreException.class),
                    hasMessage(containsString("Multiple key/value mappings found under secret path"))));
            store.retrieveEncryptionKeys();
        });
    }

    @Test
    public void testInvalidToken() throws Exception {
        doTestWithServer(vaultSecureStoreConfig -> {
            vaultSecureStoreConfig.setSecretPath(MockVaultServer.SECRET_PATH).setToken("invalid");
            VaultSecureStore store = new VaultSecureStore(vaultSecureStoreConfig, getNode());
            expectedException.expect(SecureStoreException.class);
            expectedException.expectMessage(containsString("Failed to retrieve encryption keys"));
            expectedException.expectCause(allOf(isA(SecureStoreException.class), hasMessage(containsString("statusCode: 403"))));
            store.retrieveEncryptionKeys();
        });
    }

    @Test
    public void testInvalidSecretPath() throws Exception {
        doTestWithServer(vaultSecureStoreConfig -> {
            vaultSecureStoreConfig.setSecretPath("invalid").setToken(MockVaultServer.TOKEN);
            VaultSecureStore store = new VaultSecureStore(vaultSecureStoreConfig, getNode());
            expectedException.expect(SecureStoreException.class);
            expectedException.expectMessage(containsString("Failed to retrieve encryption keys"));
            expectedException.expectCause(allOf(isA(SecureStoreException.class), hasMessage(containsString("statusCode: 403"))));
            store.retrieveEncryptionKeys();
        });
    }

    @Test
    public void testNonKVSecretsEngine() throws Exception {
        doTestWithServer(vaultSecureStoreConfig -> {
            vaultSecureStoreConfig.setSecretPath(MockVaultServer.SECRET_PATH_NONKV).setToken(MockVaultServer.TOKEN);
            VaultSecureStore store = new VaultSecureStore(vaultSecureStoreConfig, getNode());
            expectedException.expect(SecureStoreException.class);
            expectedException.expectMessage(containsString("Failed to retrieve encryption keys"));
            expectedException.expectCause(
                    allOf(isA(SecureStoreException.class), hasMessage(containsString("Unsupported secrets engine type"))));
            store.retrieveEncryptionKeys();
        });
    }

    @Test
    public void testRetrieveEncryptionKeys() throws Exception {
        doTestWithServer(vaultSecureStoreConfig -> {
            vaultSecureStoreConfig.setSecretPath(MockVaultServer.SECRET_PATH).setToken(MockVaultServer.TOKEN);
            VaultSecureStore store = new VaultSecureStore(vaultSecureStoreConfig, getNode());
            List<byte[]> keys = store.retrieveEncryptionKeys();
            assertNotNull(keys);
            assertEquals(2, keys.size());
            assertArrayEquals(MockVaultServer.KEY2, keys.get(0));
            assertArrayEquals(MockVaultServer.KEY1, keys.get(1));
        });
    }

    @Test
    public void testRetrieveEncryptionKeys_whenV1() throws Exception {
        doTestWithServer(vaultSecureStoreConfig -> {
            vaultSecureStoreConfig.setSecretPath(MockVaultServer.SECRET_PATH_V1).setToken(MockVaultServer.TOKEN);
            VaultSecureStore store = new VaultSecureStore(vaultSecureStoreConfig, getNode());
            List<byte[]> keys = store.retrieveEncryptionKeys();
            assertNotNull(keys);
            assertEquals(1, keys.size());
            assertArrayEquals(MockVaultServer.KEY2, keys.get(0));
        });
    }

    private void doTestWithServer(Consumer<VaultSecureStoreConfig> test) throws Exception {
        doTestWithServer((config, vault) -> test.accept(config));
    }

    private void doTestWithServer(BiConsumer<VaultSecureStoreConfig, MockVaultServer> test) throws Exception {
        MockVaultServer vault = null;
        try {
            VaultSecureStoreConfig vaultConfig = new VaultSecureStoreConfig("http://dummy", "dummy", "dummy");
            vault = start(maybeHttps(), vaultConfig);
            test.accept(vaultConfig, vault);
        } finally {
            if (vault != null) {
                vault.stop();
            }
        }
    }

    @Test
    public void testHTTPS_whenNoSSLConfig() {
        VaultSecureStoreConfig config = new VaultSecureStoreConfig("https://foo", "bar", "baz");
        doTestNoSSL(config);
    }

    @Test
    public void testHTTPS_whenSSLConfigDisabled() {
        VaultSecureStoreConfig config = new VaultSecureStoreConfig("https://foo", "bar", "baz").setSSLConfig(new SSLConfig());
        doTestNoSSL(config);
    }

    private void doTestNoSSL(VaultSecureStoreConfig config) {
        VaultSecureStore store = new VaultSecureStore(config, getNode());
        expectedException.expect(SecureStoreException.class);
        expectedException.expectMessage(containsString("Failed to retrieve encryption keys"));
        expectedException.expectCause(
                allOf(isA(SecureStoreException.class), hasMessage(containsString("SSL/TLS not enabled in the configuration"))));
        store.retrieveEncryptionKeys();
    }

    private File maybeHttps() throws IOException {
        return https ? new File(tf.newFolder(), "keystore") : null;
    }

    @Test
    public void testWatchChanges() throws Exception {
        doTestWithServer((vaultSecureStoreConfig, vault) -> {
            Node node = getNode();
            vaultSecureStoreConfig.setSecretPath(MockVaultServer.SECRET_PATH).setToken(MockVaultServer.TOKEN);
            vaultSecureStoreConfig.setPollingInterval(2);
            try {
                byte[] s1 = new byte[]{1};
                String s1Base64 = Base64.getEncoder().encodeToString(s1);
                byte[] s2 = new byte[]{2};
                String s2Base64 = Base64.getEncoder().encodeToString(s2);

                assertVaultChange(() -> new VaultSecureStore(vaultSecureStoreConfig, node),
                        () -> {
                            vault.addMappingVersion(MockVaultServer.SECRET_PATH, s1Base64);
                            vault.addMappingVersion(MockVaultServer.SECRET_PATH, s2Base64);
                        }, s2);

                byte[] s3 = new byte[]{3};
                String s3Base64 = Base64.getEncoder().encodeToString(s3);
                assertVaultChange(() -> new VaultSecureStore(vaultSecureStoreConfig, node),
                        () -> vault.addMappingVersion(MockVaultServer.SECRET_PATH, s3Base64), s3);
            } finally {
                vault.revertMappings();
            }
        });
    }

    private static void assertVaultChange(Supplier<VaultSecureStore> storeFn, Runnable rotateFn, byte[] expectedKey) {
        CountDownLatch latch = new CountDownLatch(2);
        AtomicReference<byte[]> newKey = new AtomicReference<>();
        VaultSecureStore store = storeFn.get();
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
            rotateFn.run();
            LOGGER.info("Rotated keys...");
            assertTrue(latch.await(20, TimeUnit.SECONDS));
            assertNotNull(newKey.get());
            assertArrayEquals(expectedKey, newKey.get());
        } catch (InterruptedException e) {
            store.dispose();
            throw new AssertionError(e);
        }
    }

    /**
     * Test that the Vault Secure Store can talk to a real Vault server. The test will run only if the following system properties
     * are set:
     * <ul>
     * <li>test.vault.token - the access token</li>
     * <li>test.vault.secretPath - the secret path where encryption keys are stored</li>
     * <li>test.vault.httpAddress - the HTTP address that Vault listens at</li>
     * </ul>
     * To test also in HTTP mode, the following system properties must be set:
     * <ul>
     * <li>test.vault.httpsAddress - the HTTPS address that Vault listens at</li>
     * <li>test.vault.caCertFile - the path to the CA certificate bundle</li>
     * </ul>
     * Note: no encryption keys need to be stored un der the specified secret path.
     */
    @Test
    public void testRealVault() throws Exception {
        String token = System.getProperty("test.vault.token");
        String secretPath = System.getProperty("test.vault.secretPath");
        String httpAddress = System.getProperty("test.vault.httpAddress");
        String httpsAddress = System.getProperty("test.vault.httpsAddress");
        String caCertFilePath = System.getProperty("test.vault.caCertFile");

        assumeThat("Vault integration not enabled. Use the following system properties to integrate with an external "
                + "Vault instance: test.vault.token, test.vault.secretPath, test.vault.httpAddress, and "
                + "(optionally, for HTTPS) test.vault.httpsAddress, test.vault.caCertFile", token, notNullValue());

        assumeThat("test.vault.http" + (https ? "s" : "") + "Address not set", https ? httpsAddress : httpAddress,
                notNullValue());

        LOGGER.info("Using Vault parameters:");
        LOGGER.info("token: " + token);
        LOGGER.info("secretPath: " + secretPath);
        LOGGER.info("httpAddress: " + httpAddress);
        LOGGER.info("httpsAddress: " + httpsAddress);
        LOGGER.info("caCertFilePath: " + caCertFilePath);

        VaultSecureStoreConfig config = new VaultSecureStoreConfig(https ? httpsAddress : httpAddress, secretPath, token);

        if (https) {
            assumeThat("Vault HTTPS integration not enabled", httpsAddress, notNullValue());
            File caCertFile = new File(caCertFilePath);
            File keyStoreFile = createKeyStoreFromPEM(caCertFile);
            SSLConfig sslConfig = new SSLConfig();
            sslConfig.setProperty("keyStore", keyStoreFile.getAbsolutePath());
            sslConfig.setProperty("keyStorePassword", "password");
            sslConfig.setProperty("keyManagerAlgorithm", "SunX509");
            sslConfig.setProperty("keyStoreType", "PKCS12");
            sslConfig.setProperty("trustStore", keyStoreFile.getAbsolutePath());
            sslConfig.setProperty("trustStorePassword", "password");
            sslConfig.setProperty("trustManagerAlgorithm", "SunX509");
            sslConfig.setProperty("trustStoreType", "PKCS12");
            sslConfig.setEnabled(true);
            config.setSSLConfig(sslConfig);
        }
        VaultSecureStore store = new VaultSecureStore(config, getNode());
        List<byte[]> keys = store.retrieveEncryptionKeys();
        assertNotNull(keys);
        assertEquals(1, keys.size());
    }

    private File createKeyStoreFromPEM(File caCertFile) throws Exception {
        File keyStoreFile = new File(tf.getRoot(), "keystore.p12");

        FileInputStream fis = new FileInputStream(caCertFile);
        X509Certificate ca = (X509Certificate) CertificateFactory.getInstance("X.509")
                                                                 .generateCertificate(new BufferedInputStream(fis));

        KeyStore ks = KeyStore.getInstance("PKCS12");
        ks.load(null, null);
        ks.setCertificateEntry("cert", ca);
        ks.store(new FileOutputStream(keyStoreFile), "password".toCharArray());
        return keyStoreFile;
    }
}
