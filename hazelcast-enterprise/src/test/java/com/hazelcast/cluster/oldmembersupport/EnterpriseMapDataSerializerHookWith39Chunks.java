package com.hazelcast.cluster.oldmembersupport;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.map.impl.operation.EnterpriseMapDataSerializerHook;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.impl.VersionedDataSerializableFactory;

public final class EnterpriseMapDataSerializerHookWith39Chunks
        implements DataSerializerHook {

    @Override
    public int getFactoryId() {
        return EnterpriseMapDataSerializerHook.F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        EnterpriseMapDataSerializerHook hook = new EnterpriseMapDataSerializerHook();
        VersionedDataSerializableFactory delegate = (VersionedDataSerializableFactory) hook.createFactory();
        return new VersionedDataSerializableFactoryWith39Chunks(delegate);
    }
}
