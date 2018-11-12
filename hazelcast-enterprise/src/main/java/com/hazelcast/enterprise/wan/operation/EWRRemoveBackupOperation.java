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
 * Removes backup of a WAN event.
 */
public class EWRRemoveBackupOperation extends EWRBaseOperation implements BackupOperation, IdentifiedDataSerializable {

    private Data event;

    public EWRRemoveBackupOperation() {
    }

    public EWRRemoveBackupOperation(String wanReplicationName, String targetName, Data event) {
        super(wanReplicationName, targetName);
        this.event = event;
    }

    @Override
    public void run() throws Exception {
        WanReplicationEndpoint endpoint = getEWRService().getEndpointOrNull(wanReplicationName, wanPublisherId);
        if (endpoint != null) {
            // the endpoint may be null in cases where the backup does
            // not contain the same config as the primary.
            // For instance, this can happen when dynamically adding new
            // WAN config during runtime and there is a race between
            // config addition and WAN replication.
            endpoint.removeBackup(getNodeEngine().<WanReplicationEvent>toObject(event));
        } else {
            getLogger().finest("Ignoring backup since WAN config doesn't exist with config name "
                    + wanReplicationName + " and publisher ID " + wanPublisherId);
        }
        response = true;
    }

    @Override
    public int getId() {
        return EWRDataSerializerHook.EWR_REMOVE_BACKUP_OPERATION;
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
