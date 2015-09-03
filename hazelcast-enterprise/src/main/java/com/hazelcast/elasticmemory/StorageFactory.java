package com.hazelcast.elasticmemory;

import com.hazelcast.internal.storage.Storage;

public interface StorageFactory {

    Storage createStorage();
}
