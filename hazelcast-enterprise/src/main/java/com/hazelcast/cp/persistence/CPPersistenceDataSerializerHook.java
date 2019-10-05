package com.hazelcast.cp.persistence;

import com.hazelcast.cp.persistence.operation.PublishLocalCPMemberOp;
import com.hazelcast.cp.persistence.operation.PublishRestoredCPMembersOp;
import com.hazelcast.cp.persistence.raftop.VerifyRestartedCPMemberOp;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

@SuppressWarnings("checkstyle:declarationorder")
public class CPPersistenceDataSerializerHook implements DataSerializerHook {

    private static final int CP_DS_FACTORY_ID = -1009;
    private static final String CP_DS_FACTORY = "hazelcast.serialization.ds.cp.persistence";

    public static final int F_ID = FactoryIdHelper.getFactoryId(CP_DS_FACTORY, CP_DS_FACTORY_ID);

    public static final int VERIFY_RESTARTED_MEMBER_OP = 1;
    public static final int PUBLISH_LOCAL_MEMBER_OP = 2;
    public static final int PUBLISH_RESTORED_MEMBERS_OP = 3;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return new DataSerializableFactory() {
            @Override
            public IdentifiedDataSerializable create(int typeId) {
                switch (typeId) {
                    case VERIFY_RESTARTED_MEMBER_OP:
                        return new VerifyRestartedCPMemberOp();
                    case PUBLISH_LOCAL_MEMBER_OP:
                        return new PublishLocalCPMemberOp();
                    case PUBLISH_RESTORED_MEMBERS_OP:
                        return new PublishRestoredCPMembersOp();
                    default:
                        throw new IllegalArgumentException("Undefined type: " + typeId);
                }
            }
        };
    }
}
