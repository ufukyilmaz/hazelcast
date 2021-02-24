package com.hazelcast.enterprise.wan.impl;

import com.hazelcast.wan.impl.InternalWanEvent;

/**
 * Finalizable extension of {@link InternalWanEvent}.
 *
 * @param <T> The type of the WAN event
 */
public interface FinalizableEnterpriseWanEvent<T> extends InternalWanEvent<T>, FinalizerAware, Finalizable {
}
