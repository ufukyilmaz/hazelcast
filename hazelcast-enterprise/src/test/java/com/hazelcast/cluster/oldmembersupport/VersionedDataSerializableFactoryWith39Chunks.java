package com.hazelcast.cluster.oldmembersupport;

import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.operation.EnterpriseMapDataSerializerHook;
import com.hazelcast.map.impl.operation.EnterpriseMapReplicationOperation39;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.VersionedDataSerializableFactory;
import com.hazelcast.version.Version;

public class VersionedDataSerializableFactoryWith39Chunks
        implements VersionedDataSerializableFactory {

    private final VersionedDataSerializableFactory delegate;

    public VersionedDataSerializableFactoryWith39Chunks(VersionedDataSerializableFactory delegate) {
        this.delegate = delegate;
    }

    @Override
    public IdentifiedDataSerializable create(int typeId, Version version) {
        if (typeId == MapDataSerializerHook.POST_JOIN_MAP_OPERATION) {
            return new PostJoinMapOperation39();
        } else if (typeId == EnterpriseMapDataSerializerHook.MAP_REPLICATION) {
            return new EnterpriseMapReplicationOperation39();
        } else {
            if (version == null) {
                return delegate.create(typeId);
            } else {
                return delegate.create(typeId, version);
            }
        }
    }

    @Override
    public IdentifiedDataSerializable create(int typeId) {
        return create(typeId, null);
    }
}
