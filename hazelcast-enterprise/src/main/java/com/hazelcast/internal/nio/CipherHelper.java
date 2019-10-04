package com.hazelcast.internal.nio;

import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.internal.util.BasicSymmetricCipherBuilder;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.PBEParameterSpec;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.Security;
import java.security.spec.AlgorithmParameterSpec;
import java.security.spec.KeySpec;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.StringUtil.stringToBytes;

public final class CipherHelper {

    private static final ILogger LOGGER = Logger.getLogger(CipherHelper.class);

    static {
        initBouncySecurityProvider();
    }

    private CipherHelper() {
    }

    public static Cipher createSymmetricReaderCipher(SymmetricEncryptionConfig config) {
        return createSymmetricReaderCipher(config, null);
    }

    public static Cipher createSymmetricReaderCipher(SymmetricEncryptionConfig config, Connection connection) {
        return createCipher(config, connection, false, "Symmetric Cipher for ReadHandler cannot be initialized");
    }

    public static Cipher createSymmetricWriterCipher(SymmetricEncryptionConfig config) {
        return createSymmetricWriterCipher(config, null);
    }

    public static Cipher createSymmetricWriterCipher(SymmetricEncryptionConfig config, Connection connection) {
        return createCipher(config, connection, true, "Symmetric Cipher for WriteHandler cannot be initialized");
    }

    @SuppressWarnings("SynchronizedMethod")
    private static synchronized Cipher createCipher(SymmetricEncryptionConfig config, Connection connection, boolean createWriter,
                                                    String exceptionMessage) {
        try {
            SymmetricCipherBuilder symmetricCipherBuilder = new SymmetricCipherBuilder(config);
            return symmetricCipherBuilder.create(createWriter, config.getKey());
        } catch (Exception e) {
            LOGGER.severe(exceptionMessage, e);
            if (connection != null) {
                connection.close(null, e);
            }
            throw rethrow(e);
        }
    }

    static void initBouncySecurityProvider() {
        try {
            if (Boolean.getBoolean("hazelcast.security.bouncy.enabled")) {
                String provider = "org.bouncycastle.jce.provider.BouncyCastleProvider";
                Security.addProvider((Provider) Class.forName(provider).newInstance());
            }
        } catch (Exception e) {
            LOGGER.warning(e);
        }
    }

    /**
     * An extension of the default {@link BasicSymmetricCipherBuilder} that supports PBE in addition
     * to the default algorithms + key generation from the salt and password.
     */
    static class SymmetricCipherBuilder extends BasicSymmetricCipherBuilder {
        private final String passPhrase;
        private final int iterationCount;

        SymmetricCipherBuilder(SymmetricEncryptionConfig config) {
            super(config);
            this.passPhrase = String.valueOf(config.getPassword());
            this.iterationCount = config.getIterationCount();
        }

        @SuppressWarnings("checkstyle:magicnumber")
        public Cipher create(boolean encryptMode, byte[] keyBytes) {
            if (keyBytes == null) {
                try {
                    MessageDigest md = MessageDigest.getInstance("MD5");
                    // 32-bit digest key = pass + salt (used only if the key supplied in create() is null)
                    ByteBuffer bbPass = ByteBuffer.allocate(32);
                    bbPass.put(md.digest(stringToBytes(passPhrase)));
                    bbPass.put(saltDigest);
                    keyBytes = bbPass.array();
                } catch (NoSuchAlgorithmException e) {
                    throw new RuntimeException(e);
                }
            }
            return super.create(encryptMode, keyBytes);
        }

        @Override
        protected CipherParams createCipherParams(byte[] keyBytes) throws GeneralSecurityException {
            if (algorithm.startsWith("PBEWith")) {
                AlgorithmParameterSpec paramSpec = new PBEParameterSpec(salt, iterationCount);
                KeySpec keySpec = new PBEKeySpec(passPhrase.toCharArray(), salt, iterationCount);
                String keyAlgorithm = findKeyAlgorithm(algorithm);
                SecretKey key = SecretKeyFactory.getInstance(keyAlgorithm).generateSecret(keySpec);
                // IV_LENGTH_CBC: CBC mode requires IvParameter with 8 byte input
                return new CipherParams(IV_LENGTH_CBC, key, paramSpec);
            }
            return super.createCipherParams(keyBytes);
        }
    }
}
