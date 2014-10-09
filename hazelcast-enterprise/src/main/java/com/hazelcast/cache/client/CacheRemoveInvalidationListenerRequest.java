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

package com.hazelcast.cache.client;

import com.hazelcast.cache.CachePortableHook;
import com.hazelcast.cache.enterprise.EnterpriseCacheService;
import com.hazelcast.client.impl.client.BaseClientRemoveListenerRequest;
import com.hazelcast.client.impl.client.RetryableRequest;
import com.hazelcast.spi.EventService;

import java.security.Permission;

public class CacheRemoveInvalidationListenerRequest extends BaseClientRemoveListenerRequest implements RetryableRequest {

    public CacheRemoveInvalidationListenerRequest() {
    }

    public CacheRemoveInvalidationListenerRequest(String name, String registrationId) {
        super(name, registrationId);
    }

    @Override
    public Object call() {
        EventService eventService = getClientEngine().getEventService();
        eventService.deregisterListener(EnterpriseCacheService.SERVICE_NAME, name, registrationId);
        return Boolean.TRUE;
    }

    public String getServiceName() {
        return EnterpriseCacheService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return CachePortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CachePortableHook.REMOVE_INVALIDATION_LISTENER;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }
}
