package com.hazelcast.elasticmemory;

import com.hazelcast.storage.Storage;

public interface StorageFactory {
	
	Storage createStorage();

}
