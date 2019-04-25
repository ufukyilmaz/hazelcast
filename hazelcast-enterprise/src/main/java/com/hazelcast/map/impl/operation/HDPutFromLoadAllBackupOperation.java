package com.hazelcast.map.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Runs on backups.
 *
 * @see PutFromLoadAllOperation
 */
public class HDPutFromLoadAllBackupOperation extends HDMapOperation implements BackupOperation {

    private List<Data> keyValueSequence;

    public HDPutFromLoadAllBackupOperation() {
        keyValueSequence = Collections.emptyList();
    }

    public HDPutFromLoadAllBackupOperation(String name, List<Data> keyValueSequence) {
        super(name);
        this.keyValueSequence = keyValueSequence;
    }

    @Override
    protected void runInternal() {
        final List<Data> keyValueSequence = this.keyValueSequence;
        if (keyValueSequence == null || keyValueSequence.isEmpty()) {
            return;
        }
        for (int i = 0; i < keyValueSequence.size(); i += 2) {
            final Data key = keyValueSequence.get(i);
            final Data value = keyValueSequence.get(i + 1);
            final Object object = mapServiceContext.toObject(value);
            recordStore.putFromLoadBackup(key, object);

            // the following check is for the case when the putFromLoad does not put the data due to various reasons
            // one of the reasons may be size eviction threshold has been reached
            if (!recordStore.existInMemory(key)) {
                continue;
            }

            publishLoadAsWanUpdate(key, value);
        }
    }

    @Override
    public void afterRun() throws Exception {
        super.afterRun();
        evict(null);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        final List<Data> keyValueSequence = this.keyValueSequence;
        final int size = keyValueSequence.size();
        out.writeInt(size);
        for (Data data : keyValueSequence) {
            out.writeData(data);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        final int size = in.readInt();
        if (size < 1) {
            keyValueSequence = Collections.emptyList();
        } else {
            final List<Data> tmpKeyValueSequence = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                Data data = in.readData();
                tmpKeyValueSequence.add(data);
            }
            keyValueSequence = tmpKeyValueSequence;
        }
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.PUT_FROM_LOAD_ALL_BACKUP;
    }
}
