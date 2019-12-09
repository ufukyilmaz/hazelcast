package com.hazelcast.enterprise.wan.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;

import java.io.IOException;

/**
 * Base class for backup aware WAN operations.
 */
public abstract class WanBackupAwareOperation extends WanBaseOperation implements BackupAwareOperation {

    int backupCount = 1;

    protected WanBackupAwareOperation() {
    }

    protected WanBackupAwareOperation(String wanReplicationName, String targetName, int backupCount) {
        super(wanReplicationName, targetName);
        this.backupCount = backupCount;
    }

    @Override
    public int getSyncBackupCount() {
        return backupCount;
    }

    @Override
    public int getAsyncBackupCount() {
        return 0;
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        backupCount = in.readInt();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(backupCount);
    }
}
