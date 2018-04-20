package com.hazelcast.cluster.oldmembersupport;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.impl.VersionedDataSerializableFactory;

public final class MapDataSerializerHookWith39Chunks
        implements DataSerializerHook {

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        MapDataSerializerHook hook = new MapDataSerializerHook();
        VersionedDataSerializableFactory delegate = (VersionedDataSerializableFactory) hook.createFactory();
        return new VersionedDataSerializableFactoryWith39Chunks(delegate);
    }
}
