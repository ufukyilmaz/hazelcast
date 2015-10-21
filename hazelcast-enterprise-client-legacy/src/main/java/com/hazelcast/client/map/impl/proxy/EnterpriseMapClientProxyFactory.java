/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.map.impl.proxy;

import com.hazelcast.cache.impl.nearcache.NearCache;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.map.impl.nearcache.ClientHDNearCacheRegistry;
import com.hazelcast.client.spi.ClientExecutionService;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.ClientProxyFactory;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.impl.utils.Registry;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;

/**
 * Creates proxy instances for client according to given configuration.
 */
public class EnterpriseMapClientProxyFactory implements ClientProxyFactory {

    private final Registry<String, NearCache> hdNearCacheRegistry;
    private final ClientConfig clientConfig;

    public EnterpriseMapClientProxyFactory(ClientExecutionService executionService,
                                           SerializationService serializationService,
                                           ClientConfig clientConfig) {
        this.clientConfig = clientConfig;
        this.hdNearCacheRegistry = new ClientHDNearCacheRegistry(executionService, serializationService, clientConfig);
    }

    @Override
    public ClientProxy create(String id) {
        NearCacheConfig nearCacheConfig = clientConfig.getNearCacheConfig(id);
        if (nearCacheConfig != null) {
            return new EnterpriseNearCachedClientMapProxyImpl(SERVICE_NAME, id, hdNearCacheRegistry);
        } else {
            return new EnterpriseClientMapProxyImpl(SERVICE_NAME, id);
        }
    }

}
