package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.ArrayDataSerializableFactory;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.ConstructorFunction;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.ENTERPRISE_HOTRESTART_CLUSTER_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.ENTERPRISE_HOTRESTART_CLUSTER_DS_FACTORY_ID;

/**
 * Serializer hook for HotRestart-related operations
 *
 */
public class HotRestartClusterSerializerHook implements DataSerializerHook {

    // Checkstyle moans about lack of JavaDoc on these fields.
    // It's just mechanical boiler-plate, there is no point in having JavaDoc
    // let's disable CheckStyle instead of writing some mambo-jambo
    //CHECKSTYLE:OFF
    public static final int F_ID = FactoryIdHelper.getFactoryId(ENTERPRISE_HOTRESTART_CLUSTER_DS_FACTORY,
            ENTERPRISE_HOTRESTART_CLUSTER_DS_FACTORY_ID);

    public static final int ASK_FOR_LOAD_COMPLETION = 0;
    public static final int ASK_FOR_PARTITION_TABLE_VALIDATION_STATUS = 1;
    public static final int CHECK_IF_MASTER_FORCE_STARTED = 2;
    public static final int FORCE_START_MEMBER = 3;
    public static final int SEND_LOAD_COMPLETION_FOR_VALIDATION = 4;
    public static final int SEND_LOAD_COMPLETION_STATUS = 5;
    public static final int SEND_PARTITION_TABLE_FOR_VALIDATION = 6;
    public static final int SEND_PARTITION_TABLE_VALIDATION_RESULT = 7;
    public static final int TRIGGER_FORCE_START_ON_MASTER = 8;
    //CHECKSTYLE:ON

    private static final int LEN = TRIGGER_FORCE_START_ON_MASTER + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructors = new ConstructorFunction[LEN];
        constructors[ASK_FOR_LOAD_COMPLETION] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new AskForLoadCompletionStatusOperation();
            }
        };
        constructors[ASK_FOR_PARTITION_TABLE_VALIDATION_STATUS] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new AskForPartitionTableValidationStatusOperation();
            }
        };
        constructors[CHECK_IF_MASTER_FORCE_STARTED] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CheckIfMasterForceStartedOperation();
            }
        };
        constructors[FORCE_START_MEMBER] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ForceStartMemberOperation();
            }
        };
        constructors[SEND_LOAD_COMPLETION_FOR_VALIDATION] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new SendLoadCompletionForValidationOperation();
            }
        };
        constructors[SEND_LOAD_COMPLETION_STATUS] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new SendLoadCompletionStatusOperation();
            }
        };
        constructors[SEND_PARTITION_TABLE_FOR_VALIDATION] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new SendPartitionTableForValidationOperation();
            }
        };
        constructors[SEND_PARTITION_TABLE_VALIDATION_RESULT] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new SendPartitionTableValidationResultOperation();
            }
        };
        constructors[TRIGGER_FORCE_START_ON_MASTER] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new TriggerForceStartOnMasterOperation();
            }
        };

        return new ArrayDataSerializableFactory(constructors);
    }
}
