package com.hazelcast.map.impl.operation;

import com.hazelcast.core.EntryView;
import com.hazelcast.map.impl.EntryViews;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.serialization.Data;

public class HDSetTTLBackupOperation extends HDKeyBasedMapOperation {

    public HDSetTTLBackupOperation() {

    }

    public HDSetTTLBackupOperation(String name, Data dataKey, long ttl) {
        super(name, dataKey, ttl);
    }

    @Override
    protected void runInternal() {
        recordStore.setTTL(dataKey, ttl);
    }

    @Override
    public void afterRun() throws Exception {
        super.afterRun();
        Record record = recordStore.getRecord(dataKey);
        if (record == null) {
            return;
        }
        if (mapContainer.isWanReplicationEnabled()) {
            EntryView entryView = EntryViews.createSimpleEntryView(dataKey, mapServiceContext.toData(record.getValue()), record);
            mapEventPublisher.publishWanUpdate(name, entryView);
        }
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.SET_TTL_BACKUP;
    }
}
