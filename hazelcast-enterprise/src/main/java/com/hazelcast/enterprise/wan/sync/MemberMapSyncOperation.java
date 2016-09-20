package com.hazelcast.enterprise.wan.sync;

import com.hazelcast.core.EntryView;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.map.impl.EntryViews;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.operation.EnterpriseMapDataSerializerHook;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.ReadonlyOperation;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Iniatiates the WAN synchronization of an {@link com.hazelcast.core.IMap} instance
 * on owned partitions of a member.
 */
public class MemberMapSyncOperation extends MapOperation implements ReadonlyOperation {

    private SyncResult result;
    private String wanReplicationName;
    private String targetGroupName;

    public MemberMapSyncOperation() {

    }

    public MemberMapSyncOperation(String wanReplicationName,
                                  String targetGroupName,
                                  String mapName) {
        this.name = mapName;
        this.wanReplicationName = wanReplicationName;
        this.targetGroupName = targetGroupName;
    }

    @Override
    public void run() throws Exception {
        result = syncLocalPartitions();
    }

    public SyncResult syncLocalPartitions() {
        Collection<Integer> ownedPartitions = mapServiceContext.getOwnedPartitions();
        SyncResult syncResult = startSyncLocalPartitions(ownedPartitions);

        return syncResult;
    }

    private SyncResult startSyncLocalPartitions(Collection<Integer> partitions) {
        SyncResult result = new SyncResult();
        for (Integer partition : partitions) {
            PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(partition);
            Iterator<Record> iterator =
                    partitionContainer.getRecordStore(name).loadAwareIterator(Clock.currentTimeMillis(), false);
            List<EntryView> records = new ArrayList<EntryView>();
            while (iterator.hasNext()) {
                Record record = iterator.next();
                EntryView entryView =
                        EntryViews.createSimpleEntryView(record.getKey(), record.getValue(), record);
                records.add(entryView);
            }
            EnterpriseWanReplicationService wanReplicationService =
                    (EnterpriseWanReplicationService) getNodeEngine().getWanReplicationService();
            PartitionSyncReplicationEventObject eventObject = new PartitionSyncReplicationEventObject(
                    wanReplicationName, targetGroupName, name, partition, records);
            wanReplicationService.publishMapWanSyncEvent(eventObject);
            result.getPartitionIds().add(partition);
        }
        return result;
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
        this.wanReplicationName = in.readUTF();
        this.targetGroupName = in.readUTF();
    }

    @Override
    public int getFactoryId() {
        return EnterpriseMapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.MEMBER_MAP_SYNC;
    }
}
