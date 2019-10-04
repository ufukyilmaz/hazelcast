package com.hazelcast.internal.hotrestart.impl.encryption;

import java.util.List;
import java.util.function.Supplier;

/**
 * A helper interface used to bootstrap the {@link EncryptionManager} with
 * the initial collection of encryption keys.
 */
public interface InitialKeysSupplier extends Supplier<List<byte[]>> {
}
