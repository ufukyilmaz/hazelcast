package com.hazelcast.cache.impl.wan;

import com.hazelcast.enterprise.wan.impl.FinalizableEnterpriseWanEvent;
import com.hazelcast.enterprise.wan.impl.Finalizer;

import javax.annotation.Nonnull;

public abstract class FinalizableWanEnterpriseCacheEvent<T> extends WanEnterpriseCacheEvent<T>
        implements FinalizableEnterpriseWanEvent<T> {
    private volatile Finalizer finalizer;

    public FinalizableWanEnterpriseCacheEvent() {
    }

    public FinalizableWanEnterpriseCacheEvent(@Nonnull String cacheName,
                                              @Nonnull String managerPrefix,
                                              int backupCount) {
        super(cacheName, managerPrefix, backupCount);
    }

    @Override
    public void setFinalizer(Finalizer finalizer) {
        this.finalizer = finalizer;
    }

    @Override
    public void doFinalize() {
        if (finalizer != null) {
            finalizer.doFinalize();
        } else {
            throw new IllegalStateException("Called finalizeEvent(), but no finalizer has been set");
        }
    }
}
