package com.hazelcast.internal.hotrestart.cluster;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.ArrayDataSerializableFactory;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.internal.util.ConstructorFunction;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.ENTERPRISE_HOTRESTART_CLUSTER_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.ENTERPRISE_HOTRESTART_CLUSTER_DS_FACTORY_ID;

public class HotRestartClusterSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(ENTERPRISE_HOTRESTART_CLUSTER_DS_FACTORY,
            ENTERPRISE_HOTRESTART_CLUSTER_DS_FACTORY_ID);

    public static final int ASK_FOR_CLUSTER_START_RESULT = 0;
    public static final int ASK_FOR_EXPECTED_MEMBERS = 1;
    public static final int SEND_CLUSTER_START_RESULT = 2;
    public static final int SEND_MEMBER_CLUSTER_START_INFO = 3;
    public static final int TRIGGER_FORCE_START = 4;
    public static final int SEND_EXPECTED_MEMBERS = 5;
    public static final int SEND_EXCLUDED_MEMBER_UUIDS = 6;
    public static final int GET_CLUSTER_STATE = 7;

    private static final int LEN = GET_CLUSTER_STATE + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructors = new ConstructorFunction[LEN];

        constructors[ASK_FOR_CLUSTER_START_RESULT] = arg -> new AskForClusterStartResultOperation();
        constructors[ASK_FOR_EXPECTED_MEMBERS] = arg -> new AskForExpectedMembersOperation();
        constructors[SEND_CLUSTER_START_RESULT] = arg -> new SendClusterStartResultOperation();
        constructors[SEND_MEMBER_CLUSTER_START_INFO] = arg -> new SendMemberClusterStartInfoOperation();
        constructors[TRIGGER_FORCE_START] = arg -> new TriggerForceStartOnMasterOperation();
        constructors[SEND_EXPECTED_MEMBERS] = arg -> new SendExpectedMembersOperation();
        constructors[SEND_EXCLUDED_MEMBER_UUIDS] = arg -> new SendExcludedMemberUuidsOperation();
        constructors[GET_CLUSTER_STATE] = arg -> new GetClusterStateOperation();

        return new ArrayDataSerializableFactory(constructors);
    }
}
