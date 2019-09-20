package com.hazelcast.spi.hotrestart.backup;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.ArrayDataSerializableFactory;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.internal.util.ConstructorFunction;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.ENTERPRISE_HOTRESTART_BACKUP_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.ENTERPRISE_HOTRESTART_BACKUP_DS_FACTORY_ID;

public class HotRestartBackupSerializerHook implements DataSerializerHook {
    public static final int F_ID = FactoryIdHelper.getFactoryId(ENTERPRISE_HOTRESTART_BACKUP_DS_FACTORY,
            ENTERPRISE_HOTRESTART_BACKUP_DS_FACTORY_ID);

    public static final int BACKUP_TRANSACTION_LOG_RECORD = 0;
    public static final int BACKUP = 1;
    public static final int BACKUP_INTERRUPT = 2;

    private static final int LEN = BACKUP_INTERRUPT + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        @SuppressWarnings("unchecked")
        final ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructors = new ConstructorFunction[LEN];
        constructors[BACKUP_TRANSACTION_LOG_RECORD] = arg -> new HotRestartBackupTransactionLogRecord();
        constructors[BACKUP] = arg -> new HotRestartBackupOperation();
        constructors[BACKUP_INTERRUPT] = arg -> new HotRestartBackupInterruptOperation();
        return new ArrayDataSerializableFactory(constructors);
    }
}
