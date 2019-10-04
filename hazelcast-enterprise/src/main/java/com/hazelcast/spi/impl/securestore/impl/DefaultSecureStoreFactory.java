package com.hazelcast.spi.impl.securestore.impl;

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.JavaKeyStoreSecureStoreConfig;
import com.hazelcast.config.SecureStoreConfig;
import com.hazelcast.config.VaultSecureStoreConfig;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.spi.impl.securestore.SecureStore;
import com.hazelcast.spi.impl.securestore.SecureStoreFactory;

import javax.annotation.Nonnull;

/**
 * Default {@link SecureStoreFactory} implementation.
 * <p>
 * Supports Java KeyStore and HashiCorp Vault Secure Stores.
 */
public class DefaultSecureStoreFactory implements SecureStoreFactory {

    private final Node node;

    public DefaultSecureStoreFactory(@Nonnull Node node) {
        this.node = node;
    }

    @Nonnull
    public SecureStore getSecureStore(@Nonnull SecureStoreConfig secureStoreConfig) {
        if (secureStoreConfig instanceof JavaKeyStoreSecureStoreConfig) {
            return new JavaKeyStoreSecureStore((JavaKeyStoreSecureStoreConfig) secureStoreConfig, node);
        } else if (secureStoreConfig instanceof VaultSecureStoreConfig) {
            return new VaultSecureStore((VaultSecureStoreConfig) secureStoreConfig, node);
        }
        throw new InvalidConfigurationException("Unsupported Secure Store configuration type: " + secureStoreConfig);
    }
}
