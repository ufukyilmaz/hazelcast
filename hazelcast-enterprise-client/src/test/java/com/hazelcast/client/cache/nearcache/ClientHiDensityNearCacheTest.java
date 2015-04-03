/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.client.cache.nearcache;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.annotation.QuickTest;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class ClientHiDensityNearCacheTest extends ClientNearCacheTestSupport {

    private static final MemorySize SERVER_NATIVE_MEMORY_SIZE = new MemorySize(256, MemoryUnit.MEGABYTES);
    private static final MemorySize CLIENT_NATIVE_MEMORY_SIZE = new MemorySize(128, MemoryUnit.MEGABYTES);

    protected Config createConfig() {
        Config config = super.createConfig();
        NativeMemoryConfig nativeMemoryConfig =
                new NativeMemoryConfig()
                        .setSize(SERVER_NATIVE_MEMORY_SIZE)
                        .setEnabled(true);
        config.setNativeMemoryConfig(nativeMemoryConfig);
        return config;
    }

    protected ClientConfig createClientConfig() {
        ClientConfig clientConfig = super.createClientConfig();
        NativeMemoryConfig nativeMemoryConfig =
                new NativeMemoryConfig()
                        .setSize(CLIENT_NATIVE_MEMORY_SIZE)
                        .setEnabled(true);
        clientConfig.setNativeMemoryConfig(nativeMemoryConfig);
        return clientConfig;
    }

    @Test
    public void putAndGetFromCacheAndThenGetFromClientHiDensityNearCacheWithNativeInMemoryFormat() {
        putAndGetFromCacheAndThenGetFromClientNearCache(InMemoryFormat.NATIVE);
    }

    @Test
    public void putToCacheAndThenGetFromClientHiDensityNearCacheWithNativeInMemoryFormat() {
        putToCacheAndThenGetFromClientNearCache(InMemoryFormat.NATIVE);
    }

    @Test
    public void putToCacheAndUpdateFromOtherNodeThenGetUpdatedFromClientHiDensityNearCacheWithNativeInMemoryFormat() {
        putToCacheAndUpdateFromOtherNodeThenGetUpdatedFromClientNearCache(InMemoryFormat.NATIVE);
    }

    @Test
    public void putToCacheAndRemoveFromOtherNodeThenCantGetUpdatedFromClientHiDensityNearCacheWithNativeInMemoryFormat() {
        putToCacheAndRemoveFromOtherNodeThenCantGetUpdatedFromClientNearCache(InMemoryFormat.NATIVE);
    }

    @Test
    public void putToCacheAndClearOrDestroyThenCantGetAnyRecordFromClientHiDensityNearCacheWithNativeInMemoryFormat() {
        putToCacheAndClearOrDestroyThenCantGetAnyRecordFromClientNearCache(InMemoryFormat.NATIVE);
    }

}
