package com.hazelcast.internal.hotrestart.impl.encryption;

import com.hazelcast.config.AbstractSymmetricEncryptionConfig;
import com.hazelcast.internal.util.BasicSymmetricCipherBuilder;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

/**
 * An extension of {@link BasicSymmetricCipherBuilder} that does not permit
 * ciphers with no padding.
 */
public class HotRestartCipherBuilder extends BasicSymmetricCipherBuilder {

    public HotRestartCipherBuilder(AbstractSymmetricEncryptionConfig config) {
        super(config);
    }

    @Override
    protected CipherParams createCipherParams(byte[] keyBytes) throws GeneralSecurityException {
        /* we do not support ciphers with no padding (because we would have to add/remove
           the padding of the last block manually) */
        if (algorithm.endsWith("/NoPadding")) {
            return null;
        }
        return super.createCipherParams(keyBytes);
    }

    /**
     * Generates an encryption key.
     *
     * @param keySize the key size (fall-back to a default size in case of a non-positive value)
     * @return the generated key bytes
     */
    byte[] generateKey(int keySize) throws NoSuchAlgorithmException {
        KeyGenerator keyGenerator = KeyGenerator.getInstance(findKeyAlgorithm(algorithm));
        SecureRandom random = new SecureRandom();
        if (keySize > 0) {
            keyGenerator.init(keySize, random);
        } else {
            keyGenerator.init(random);
        }
        SecretKey secretKey = keyGenerator.generateKey();
        return secretKey.getEncoded();
    }

}
