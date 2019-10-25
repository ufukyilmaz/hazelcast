package com.hazelcast.spi.impl.securestore.impl;

import com.hazelcast.config.JavaKeyStoreSecureStoreConfig;
import com.hazelcast.internal.util.StringUtil;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;

public class TestJavaKeyStoreSecureStoreUtils {

    public static final String KEYSTORE_PASSWORD = "password";
    public static final String KEYSTORE_TYPE_PKCS12 = "PKCS12";
    public static final byte[] KEY_BYTES = StringUtil.stringToBytes("0123456789012345");

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

    private TestJavaKeyStoreSecureStoreUtils() {
    }

    /**
     * Creates a Java KeyStore with specified type, password, and keys.
     * <p>
     * The only reason why this method is synchronized is to eliminate the race
     * condition in OpenJDK's sun.security.x509.AlgorithmId.oidTable initialization
     * (see https://bugs.openjdk.java.net/browse/JDK-8156584, fixed in OpenJDK 9
     * and higher). If multiple KeyStores are created concurrently, it may trigger
     * the above issue, causing unexpected test failures.
     *
     * @return a {@link JavaKeyStoreSecureStoreConfig} corresponding to the created KeyStore
     */
    public static synchronized JavaKeyStoreSecureStoreConfig createJavaKeyStore(File path, String type, String password,
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
            /* IBM's implementations throws a NPE when opening an empty PKCS12 store
               (will be fixed in 8.0 SR6). The following is a workaround for older
               versions. We insert a dummy certificate entry (which will be ignored
               by the Java KeyStore Secure Store implementation). */
            String vendor = System.getProperty("java.vendor");
            if (vendor != null && vendor.toLowerCase().contains("ibm")) {
                if (keys.length == 0 && "PKCS12".equals(type)) {
                    Certificate cert = CertificateFactory.getInstance("X.509")
                            .generateCertificate(new ByteArrayInputStream(TEST_CERTIFICATE.getBytes()));
                    ks.setCertificateEntry("certificate-entry", cert);
                }
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
