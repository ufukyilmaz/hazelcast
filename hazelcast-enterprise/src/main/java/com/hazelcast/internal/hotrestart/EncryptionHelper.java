package com.hazelcast.internal.hotrestart;

import com.hazelcast.config.EncryptionAtRestConfig;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.hotrestart.impl.encryption.HotRestartCipherBuilder;
import com.hazelcast.internal.hotrestart.impl.encryption.InitialKeysSupplier;
import com.hazelcast.internal.nio.Disposable;
import com.hazelcast.spi.impl.securestore.SecureStore;
import com.hazelcast.spi.impl.securestore.SecureStore.EncryptionKeyListener;
import com.hazelcast.spi.impl.securestore.impl.DefaultSecureStoreFactory;

import java.util.List;

/**
 * Helper class to encapsulate some of the Hot Restart Encryption configuration/integration logic.
 */
final class EncryptionHelper implements Disposable, InitialKeysSupplier {
    private final EncryptionAtRestConfig encryptionAtRestConfig;
    private final int keySize;
    private SecureStore secureStore;

    EncryptionHelper(EncryptionAtRestConfig encryptionAtRestConfig) {
        this.encryptionAtRestConfig = encryptionAtRestConfig;
        this.keySize = encryptionAtRestConfig.getKeySize();
    }

    /**
     * Creates a new cipher builder if encryption is enabled, returns {@code null} otherwise.
     * @return a new cipher builder or {@code null} if encryption is not enabled
     */
    HotRestartCipherBuilder newCipherBuilder() {
        return encryptionAtRestConfig.isEnabled() ? new HotRestartCipherBuilder(encryptionAtRestConfig) : null;
    }

    /**
     * Returns the size (in bits) of the Hot Restart Store encryption key.
     * @return the encryption key size
     */
    int getKeySize() {
        return keySize;
    }

    /**
     * Prepares the encryption helper for use: creates a Secure Store and registers
     * an encryption key listener to watch for encryption key changes.
     * @see HotRestartIntegrationService#prepare()
     * @param node the current node
     * @param encryptionKeyListener the encryption key listener to register with the Secure Store.
     */
    void prepare(Node node, EncryptionKeyListener encryptionKeyListener) {
        secureStore = encryptionAtRestConfig.isEnabled() ? new DefaultSecureStoreFactory(node)
                .getSecureStore(encryptionAtRestConfig.getSecureStoreConfig()) : null;
        if (secureStore != null && encryptionKeyListener != null) {
            secureStore.addEncryptionKeyListener(encryptionKeyListener);
        }
    }

    @Override
    public void dispose() {
        if (secureStore instanceof Disposable) {
            ((Disposable) secureStore).dispose();
        }
        secureStore = null;
    }

    @Override
    public List<byte[]> get() {
        if (secureStore != null) {
            return secureStore.retrieveEncryptionKeys();
        }
        return null;
    }
}
