package com.hazelcast.spi.impl.securestore.impl;

import com.hazelcast.config.JavaKeyStoreSecureStoreConfig;
import com.hazelcast.internal.util.StringUtil;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.File;
import java.io.FileOutputStream;
import java.security.KeyStore;

public class TestJavaKeyStoreSecureStoreUtils {

    public static final String KEYSTORE_PASSWORD = "password";
    public static final String KEYSTORE_TYPE_PKCS12 = "PKCS12";
    public static final byte[] KEY_BYTES = StringUtil.stringToBytes("0123456789012345");

    private TestJavaKeyStoreSecureStoreUtils() {
    }

    /**
     * Creates a Java KeyStore with specified type, password, and keys.
     * @return a {@link JavaKeyStoreSecureStoreConfig} corresponding to the created KeyStore
     */
    public static JavaKeyStoreSecureStoreConfig createJavaKeyStore(File path, String type, String password,
                                                                   byte[]... keys) {
        JavaKeyStoreSecureStoreConfig config = new JavaKeyStoreSecureStoreConfig(path).setPassword(password).setType(type);
        try {
            if (path.exists()) {
                path.delete();
            }
            KeyStore ks = KeyStore.getInstance(type);
            ks.load(null, null);
            char c = 'a';
            for (byte[] key : keys) {
                SecretKey secretKey = new SecretKeySpec(key, "AES");
                KeyStore.SecretKeyEntry secret = new KeyStore.SecretKeyEntry(secretKey);
                KeyStore.ProtectionParameter entryPassword = new KeyStore.PasswordProtection(password.toCharArray());
                c++;
                ks.setEntry(String.valueOf(c), secret, entryPassword);
            }
            try (FileOutputStream out = new FileOutputStream(path)) {
                ks.store(out, password.toCharArray());
            }
        } catch (Exception e) {
            throw new AssertionError(e);
        }
        return config;
    }

}
