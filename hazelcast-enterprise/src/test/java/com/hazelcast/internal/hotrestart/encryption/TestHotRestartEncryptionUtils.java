package com.hazelcast.internal.hotrestart.encryption;

import com.hazelcast.config.Config;
import com.hazelcast.config.EncryptionAtRestConfig;
import com.hazelcast.config.JavaKeyStoreSecureStoreConfig;
import com.hazelcast.config.SecureStoreConfig;

import java.io.File;
import java.io.IOException;

import static com.hazelcast.spi.impl.securestore.impl.TestJavaKeyStoreSecureStoreUtils.KEYSTORE_PASSWORD;
import static com.hazelcast.spi.impl.securestore.impl.TestJavaKeyStoreSecureStoreUtils.KEYSTORE_TYPE_PKCS12;
import static com.hazelcast.spi.impl.securestore.impl.TestJavaKeyStoreSecureStoreUtils.KEY_BYTES;
import static com.hazelcast.spi.impl.securestore.impl.TestJavaKeyStoreSecureStoreUtils.createJavaKeyStore;

public class TestHotRestartEncryptionUtils {

    private static final String AES_CBC_PKCS5PADDING = "AES/CBC/PKCS5Padding";
    private static final String SALT = "sugar";

    private TestHotRestartEncryptionUtils() {
    }

    public static Config withBasicEncryptionAtRestConfig(Config config) {
        File keyStoreFile = createKeyStoreFile(config.getHotRestartPersistenceConfig().getBaseDir());
        JavaKeyStoreSecureStoreConfig keyStoreConfig = createJavaKeyStore(keyStoreFile, KEYSTORE_TYPE_PKCS12, KEYSTORE_PASSWORD,
                KEY_BYTES);
        return withBasicEncryptionAtRestConfig(config, keyStoreConfig);
    }

    public static Config withBasicEncryptionAtRestConfig(Config config, SecureStoreConfig secureStoreConfig) {
        EncryptionAtRestConfig encryptionAtRestConfig = config.getHotRestartPersistenceConfig().getEncryptionAtRestConfig();
        encryptionAtRestConfig.setEnabled(true);
        encryptionAtRestConfig.setAlgorithm(AES_CBC_PKCS5PADDING);
        encryptionAtRestConfig.setSalt(SALT);
        encryptionAtRestConfig.setSecureStoreConfig(secureStoreConfig);
        return config;
    }

    private static File createKeyStoreFile(File baseDir) {
        baseDir.mkdirs();
        try {
            return File.createTempFile("keystore", ".p12", baseDir);
        } catch (IOException e) {
            throw new AssertionError("Failed to create a keystore file", e);
        }
    }

}
