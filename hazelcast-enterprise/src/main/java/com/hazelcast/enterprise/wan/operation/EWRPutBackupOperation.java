package com.hazelcast.enterprise.wan.operation;

import com.hazelcast.enterprise.wan.EWRDataSerializerHook;
import com.hazelcast.enterprise.wan.WanReplicationEndpoint;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.wan.WanReplicationEvent;

import java.io.IOException;

/**
 * Publishes backup of wan event
 */
public class EWRPutBackupOperation extends EWRBaseOperation
        implements BackupOperation, IdentifiedDataSerializable {

    private Data event;

    public EWRPutBackupOperation() { }

    public EWRPutBackupOperation(String wanReplicationName,
                                 String targetName, Data event) {
        super(wanReplicationName, targetName);
        this.event = event;
    }

    @Override
    public void run() throws Exception {
        WanReplicationEndpoint endpoint = getEWRService().getEndpoint(wanReplicationName, targetName);
        endpoint.putBackup(getNodeEngine().<WanReplicationEvent>toObject(event));
        response = true;
    }

    @Override
    public int getId() {
        return EWRDataSerializerHook.EWR_PUT_BACKUP_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeData(event);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        event = in.readData();
    }
}
