package com.hazelcast.internal.hotrestart.impl.encryption;

public class HotRestartStoreEncryptionConfig {

    private HotRestartCipherBuilder cipherBuilder;
    private InitialKeysSupplier initialKeysSupplier;
    private int keySize;

    public HotRestartCipherBuilder cipherBuilder() {
        return cipherBuilder;
    }

    public HotRestartStoreEncryptionConfig setCipherBuilder(HotRestartCipherBuilder cipherBuilder) {
        this.cipherBuilder = cipherBuilder;
        return this;
    }

    public InitialKeysSupplier initialKeysSupplier() {
        return initialKeysSupplier;
    }

    public HotRestartStoreEncryptionConfig setInitialKeysSupplier(InitialKeysSupplier initialKeysSupplier) {
        this.initialKeysSupplier = initialKeysSupplier;
        return this;
    }

    public int keySize() {
        return keySize;
    }

    public HotRestartStoreEncryptionConfig setKeySize(int keySize) {
        this.keySize = keySize;
        return this;
    }

}
