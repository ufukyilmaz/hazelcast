package com.hazelcast.spi.impl.securestore.impl;

import com.hazelcast.config.JavaKeyStoreSecureStoreConfig;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.spi.impl.securestore.SecureStoreException;

import javax.annotation.Nonnull;
import javax.crypto.SecretKey;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.DigestInputStream;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Java KeyStore Secure Store implementation.
 */
public class JavaKeyStoreSecureStore extends AbstractSecureStore {

    private final JavaKeyStoreSecureStoreConfig config;

    JavaKeyStoreSecureStore(@Nonnull JavaKeyStoreSecureStoreConfig config, @Nonnull Node node) {
        super(config.getPollingInterval(), node);
        this.config = config;
    }

    private static KeyStore loadKeyStore(JavaKeyStoreSecureStoreConfig config) {
        FileInputStream in = null;
        try {
            in = new FileInputStream(config.getPath());
            KeyStore ks = KeyStore.getInstance(config.getType());
            ks.load(in, toCharArray(config.getPassword()));
            return ks;
        } catch (IOException | GeneralSecurityException e) {
            throw new SecureStoreException("Failed to load Java KeyStore", e);
        } finally {
            IOUtil.closeResource(in);
        }
    }

    @Nonnull
    @Override
    public List<byte[]> retrieveEncryptionKeys() {
        KeyStore ks = loadKeyStore(config);
        try {
            Enumeration<String> aliases = ks.aliases();
            char[] password = toCharArray(config.getPassword());
            String currentKeyAlias = config.getCurrentKeyAlias();
            Comparator<String> comparator = getComparator(currentKeyAlias);
            SortedSet<String> ordered = new TreeSet<>(comparator);
            while (aliases.hasMoreElements()) {
                String alias = aliases.nextElement();
                // consider only key entries with no certificate chain
                if (ks.isKeyEntry(alias) && ks.getCertificateChain(alias) == null) {
                    ordered.add(alias);
                }
            }
            if (currentKeyAlias != null && !ordered.isEmpty() && !currentKeyAlias.equals(ordered.iterator().next())) {
                throw new SecureStoreException("Current encryption key entry not found: " + currentKeyAlias);
            }
            List<byte[]> keys = new ArrayList<>();
            for (String alias : ordered) {
                Key key = ks.getKey(alias, password);
                assert key instanceof SecretKey;
                keys.add(key.getEncoded());
            }
            return keys;
        } catch (GeneralSecurityException e) {
            throw new SecureStoreException("Failed to retrieve encryption keys", e);
        }
    }

    private static Comparator<String> getComparator(String currentKeyAlias) {
        // if currentKeyAlias is non-null, use a Comparator that treats currentKeyAlias as smaller than the rest
        return currentKeyAlias == null ? Comparator.reverseOrder() : (o1, o2) -> {
            if (currentKeyAlias.equals(o1)) {
                return -1;
            }
            if (currentKeyAlias.equals(o2)) {
                return 1;
            }
            return o1.compareTo(o2);
        };
    }

    private byte[] retrieveCurrentEncryptionKey() {
        List<byte[]> keys = retrieveEncryptionKeys();
        return keys.isEmpty() ? null : keys.get(0);
    }

    private static char[] toCharArray(String str) {
        return str == null ? null : str.toCharArray();
    }

    @Override
    protected Runnable getWatcher() {
        return new KeyStoreWatcher();
    }

    private final class KeyStoreWatcher implements Runnable {
        private final MessageDigest md;
        private byte[] lastChecksum;
        private byte[] lastKey;

        private KeyStoreWatcher() {
            try {
                this.md = MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException e) {
                throw new SecureStoreException("Failed to construct a MessageDigest object", e);
            }
            this.lastChecksum = checksum();
            this.lastKey = retrieveCurrentEncryptionKey();
        }

        @SuppressWarnings("checkstyle:magicnumber")
        private byte[] checksum() {
            md.reset();
            byte[] buffer = new byte[1024];
            try (DigestInputStream dis = new DigestInputStream(new FileInputStream(config.getPath()), md)) {
                int len;
                do {
                    len = dis.read(buffer, 0, buffer.length);
                } while (len != -1);
                return md.digest();
            } catch (FileNotFoundException e) {
                return null;
            } catch (IOException e) {
                throw new SecureStoreException("Failed to calculate KeyStore file checksum", e);
            }
        }

        @Override
        public void run() {
            try {
                byte[] currentChecksum = checksum();
                if (currentChecksum != null && (lastChecksum == null || !Arrays.equals(lastChecksum, currentChecksum))) {
                    logger.fine("Java KeyStore change detected: " + config.getPath());
                    lastChecksum = currentChecksum;
                    byte[] key = retrieveCurrentEncryptionKey();
                    if (key != null && !Arrays.equals(key, lastKey)) {
                        logger.info("Java KeyStore encryption key change detected: " + config.getPath());
                        notifyEncryptionKeyListeners(key);
                    }
                }
            } catch (Exception e) {
                logger.warning("Error while detecting changes in Java KeyStore: " + config.getPath());
            }
        }
    }
}
