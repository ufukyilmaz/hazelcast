package com.hazelcast.enterprise.wan.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.BackupAwareOperation;

import java.io.IOException;

/**
 * Base class for backup aware wan operations
 */
public abstract class EWRBackupAwareOperation extends EWRBaseOperation implements BackupAwareOperation {

    int backupCount = 1;

    protected EWRBackupAwareOperation() { }

    protected EWRBackupAwareOperation(String wanReplicationName, String targetName, int backupCount) {
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
