package com.hazelcast.internal.hotrestart.encryption;

import com.hazelcast.config.Config;
import com.hazelcast.config.SecureStoreConfig;
import com.hazelcast.enterprise.SampleLicense;
import com.hazelcast.internal.hotrestart.HotRestartTestSupport;
import com.hazelcast.map.IMap;
import org.junit.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.function.Supplier;

import static com.hazelcast.internal.hotrestart.encryption.TestHotRestartEncryptionUtils.withBasicEncryptionAtRestConfig;
import static org.junit.Assert.fail;

public abstract class AbstractHotRestartSecureStoreIntegrationTest extends HotRestartTestSupport {

    private Config makeConfig(Supplier<SecureStoreConfig> secureStoreSupplier) {
        Config config = new Config().setLicenseKey(SampleLicense.UNLIMITED_LICENSE);
        config.getHotRestartPersistenceConfig().setEnabled(true).setBaseDir(baseDir);
        return withBasicEncryptionAtRestConfig(config, secureStoreSupplier.get());
    }

    protected abstract SecureStoreConfig createSecureStoreConfig();

    protected abstract SecureStoreConfig createSecureStoreNotAccessibleConfig();

    protected abstract SecureStoreConfig createSecureStoreUnusableKeysConfig();

    protected abstract SecureStoreConfig createSecureStoreNoKeysConfig();

    @Test
    public void testBasic() {
        IMap<String, String> map = newHazelcastInstance(() -> makeConfig(this::createSecureStoreConfig)).getMap(randomMapName());
        map.put(randomString(), randomString());
    }

    @Test
    public void testFailFast_whenSecureStoreNotAccessible() {
        try {
            newHazelcastInstance(() -> makeConfig(this::createSecureStoreNotAccessibleConfig));
            fail("Exception expected");
        } catch (Exception e) {
            startupFailure(e, "SecureStoreException");
        }
    }

    @Test
    public void testFailFast_whenEncryptionKeyUnusable() {
        try {
            newHazelcastInstance(() -> makeConfig(this::createSecureStoreUnusableKeysConfig));
            fail("Exception expected");
        } catch (Exception e) {
            startupFailure(e, "Unable to create Cipher");
        }
    }

    @Test
    public void testFailFast_whenNoKeys() {
        try {
            newHazelcastInstance(() -> makeConfig(this::createSecureStoreNoKeysConfig));
            fail("Exception expected");
        } catch (Exception e) {
            startupFailure(e, "No master encryption key available");
        }
    }

    @Test
    public void testFailFast_whenNoSuchEncryptionAlgorithm() {
        try {
            newHazelcastInstance(() -> {
                Config config = makeConfig(this::createSecureStoreConfig);
                config.getHotRestartPersistenceConfig().getEncryptionAtRestConfig().setAlgorithm("unsupported");
                return config;
            });
            fail("Exception expected");
        } catch (Exception e) {
            startupFailure(e, "NoSuchAlgorithmException");
        }
    }

    @Test
    public void testFailFast_whenEncryptionAlgorithmNotSupported() {
        try {
            newHazelcastInstance(() -> {
                Config config = makeConfig(this::createSecureStoreConfig);
                config.getHotRestartPersistenceConfig().getEncryptionAtRestConfig().setAlgorithm("AES/CBC/NoPadding");
                return config;
            });
            fail("Exception expected");
        } catch (Exception e) {
            startupFailure(e, "Encryption algorithm not supported");
        }
    }

    private static void startupFailure(Exception e, String expectedMessage) {
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        assertContains(sw.toString(), expectedMessage);
    }

}
