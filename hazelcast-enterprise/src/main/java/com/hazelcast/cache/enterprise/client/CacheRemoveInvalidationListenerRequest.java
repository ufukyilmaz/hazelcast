package com.hazelcast.cache.enterprise.client;

import com.hazelcast.cache.EnterpriseCacheService;
import com.hazelcast.client.impl.client.BaseClientRemoveListenerRequest;
import com.hazelcast.client.impl.client.RetryableRequest;
import com.hazelcast.spi.EventService;

import java.security.Permission;

/**
 * @author mdogan 18/02/14
 */
public class CacheRemoveInvalidationListenerRequest
        extends BaseClientRemoveListenerRequest
        implements RetryableRequest {

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
