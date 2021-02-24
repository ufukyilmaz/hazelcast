package com.hazelcast.map.impl.wan;

import com.hazelcast.enterprise.wan.impl.FinalizableEnterpriseWanEvent;
import com.hazelcast.enterprise.wan.impl.Finalizer;

public abstract class FinalizableWanEnterpriseMapEvent<T> extends WanEnterpriseMapEvent<T>
        implements FinalizableEnterpriseWanEvent<T> {
    private volatile Finalizer finalizer;

    public FinalizableWanEnterpriseMapEvent() {
    }

    public FinalizableWanEnterpriseMapEvent(String mapName, int backupCount) {
        super(mapName, backupCount);
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
