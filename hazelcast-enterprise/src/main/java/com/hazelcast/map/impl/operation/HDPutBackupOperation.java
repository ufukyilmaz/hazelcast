package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordInfo;
import com.hazelcast.map.impl.record.Records;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.BackupOperation;

import java.io.IOException;

public final class HDPutBackupOperation extends HDKeyBasedMapOperation implements BackupOperation,
        IdentifiedDataSerializable {

    // TODO unlockKey is a logic just used in transactional put operations.
    // TODO It complicates here there should be another Operation for that logic, e.g., TxnSetBackup
    private boolean unlockKey;
    private RecordInfo recordInfo;
    private boolean putTransient;

    public HDPutBackupOperation(String name, Data dataKey, Data dataValue, RecordInfo recordInfo, boolean putTransient) {
        this(name, dataKey, dataValue, recordInfo, false, putTransient);
    }

    public HDPutBackupOperation(String name, Data dataKey, Data dataValue,
                                RecordInfo recordInfo, boolean unlockKey, boolean putTransient) {
        this(name, dataKey, dataValue, recordInfo, unlockKey, putTransient, false);
    }

    public HDPutBackupOperation(String name, Data dataKey, Data dataValue,
                                RecordInfo recordInfo, boolean unlockKey, boolean putTransient,
                                boolean disableWanReplicationEvent) {
        super(name, dataKey, dataValue);
        this.unlockKey = unlockKey;
        this.recordInfo = recordInfo;
        this.putTransient = putTransient;
        this.disableWanReplicationEvent = disableWanReplicationEvent;
    }

    public HDPutBackupOperation() {
    }

    @Override
    protected void runInternal() {
        ttl = recordInfo != null ? recordInfo.getTtl() : ttl;
        maxIdle = recordInfo != null ? recordInfo.getMaxIdle() : maxIdle;

        Record record = recordStore.putBackup(dataKey, dataValue, ttl, maxIdle,
                putTransient, getCallerProvenance());

        if (recordInfo != null) {
            Records.applyRecordInfo(record, recordInfo);
        }
        if (unlockKey) {
            recordStore.forceUnlock(dataKey);
        }
    }

    @Override
    public void afterRun() {
        if (recordInfo != null) {
            evict(dataKey);
        }

        publishWanUpdate(dataKey, dataValue);
        disposeDeferredBlocks();
    }

    @Override
    public Object getResponse() {
        return Boolean.TRUE;
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.PUT_BACKUP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(unlockKey);
        if (recordInfo != null) {
            out.writeBoolean(true);
            recordInfo.writeData(out);
        } else {
            out.writeBoolean(false);
        }
        out.writeBoolean(putTransient);
        out.writeBoolean(disableWanReplicationEvent);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        unlockKey = in.readBoolean();
        boolean hasRecordInfo = in.readBoolean();
        if (hasRecordInfo) {
            recordInfo = new RecordInfo();
            recordInfo.readData(in);
        }
        putTransient = in.readBoolean();
        disableWanReplicationEvent = in.readBoolean();
    }

}
