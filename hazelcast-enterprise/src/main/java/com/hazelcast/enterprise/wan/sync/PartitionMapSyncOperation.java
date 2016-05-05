package com.hazelcast.enterprise.wan.sync;

import com.hazelcast.core.EntryView;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.map.impl.EntryViews;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.ReadonlyOperation;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Iniatiates the WAN synchronization of an {@link com.hazelcast.core.IMap} instance
 * on a specific partition.
 */
public class PartitionMapSyncOperation extends MapOperation implements ReadonlyOperation {

    private SyncResult result = new SyncResult();
    private String wanReplicationName;
    private String targetGroupName;

    public PartitionMapSyncOperation() { }

    public PartitionMapSyncOperation(String wanReplicationName,
                                     String targetGroupName,
                                     String mapName) {
        this.name = mapName;
        this.wanReplicationName = wanReplicationName;
        this.targetGroupName = targetGroupName;
    }

    @Override
    public void run() throws Exception {
        PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(getPartitionId());
        Iterator<Record> iterator
                = partitionContainer.getRecordStore(name).loadAwareIterator(Clock.currentTimeMillis(), false);
        List<EntryView> records = new ArrayList<EntryView>();
        while (iterator.hasNext()) {
            Record record = iterator.next();
            EntryView entryView = EntryViews.createSimpleEntryView(record.getKey(), record.getValue(), record);
            records.add(entryView);
        }
        EnterpriseWanReplicationService wanReplicationService
                = (EnterpriseWanReplicationService) getNodeEngine().getWanReplicationService();
        PartitionSyncReplicationEventObject eventObject = new PartitionSyncReplicationEventObject(
                wanReplicationName, targetGroupName, name, getPartitionId(), records);
        wanReplicationService.publishMapWanSyncEvent(eventObject);
        result.getPartitionIds().add(getPartitionId());
    }

    @Override
    public Object getResponse() {
        return result;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(wanReplicationName);
        out.writeUTF(targetGroupName);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        wanReplicationName = in.readUTF();
        targetGroupName = in.readUTF();
    }
}
