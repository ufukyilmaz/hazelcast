/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.map.impl.eviction;

import com.hazelcast.core.EntryView;
import com.hazelcast.map.impl.eviction.policies.MapEvictionPolicy;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.HDStorageImpl;
import com.hazelcast.map.impl.recordstore.HDStorageSCHM;
import com.hazelcast.map.impl.recordstore.HotRestartHDStorageImpl;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.impl.recordstore.Storage;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.IPartitionService;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.map.impl.eviction.EvictionChecker.isEvictionEnabled;

/**
 * {@link Evictor} for maps which has {@link com.hazelcast.config.InMemoryFormat#NATIVE NATIVE} in-memory-format.
 *
 * This evictor is sampling based evictor. So independent of the size of record-store, eviction works in constant time.
 */
public class HDEvictorImpl extends EvictorImpl {

    private static final int ONE_HUNDRED_PERCENT = 100;
    private static final int FORCED_EVICTION_PERCENTAGE = 20;
    private static final int MIN_FORCED_EVICTION_ENTRY_REMOVE_COUNT = 20;

    public HDEvictorImpl(EvictionChecker evictionChecker, MapEvictionPolicy evictionPolicy, IPartitionService partitionService) {
        super(evictionChecker, evictionPolicy, partitionService);
    }

    @Override
    protected Record getRecordFromEntryView(EntryView selectedEntry) {
        return ((HDStorageSCHM.LazyEntryViewFromRecord) selectedEntry).getRecord();
    }

    public void forceEvict(RecordStore recordStore) {
        if (recordStore.size() == 0 || !isEvictionEnabled(recordStore)) {
            return;
        }

        boolean backup = isBackup(recordStore);

        int removalSize = calculateRemovalSize(recordStore);
        int removedEntryCount = 0;
        Storage<Data, Record> storage = recordStore.getStorage();
        List<Record> recordsToEvict = new ArrayList<Record>(removalSize);
        for (Record record : storage.values()) {
            Data key = record.getKey();
            if (!recordStore.isLocked(key)) {
                recordsToEvict.add(record);
                removedEntryCount++;
            }

            if (removedEntryCount >= removalSize) {
                break;
            }
        }

        for (Record record : recordsToEvict) {
            if (!backup) {
                recordStore.doPostEvictionOperations(record, backup);
            }
            recordStore.evict(record.getKey(), backup);
        }

        recordStore.disposeDeferredBlocks();
    }

    @Override
    protected Iterable<EntryView> getSamples(RecordStore recordStore) {
        int sampleCount = evictionPolicy.getSampleCount();
        Storage storage = recordStore.getStorage();

        if (storage instanceof HotRestartHDStorageImpl) {
            return (Iterable<EntryView>) ((HotRestartHDStorageImpl) storage).getStorageImpl().getRandomSamples(sampleCount);
        }

        return (Iterable<EntryView>) ((HDStorageImpl) storage).getRandomSamples(sampleCount);
    }

    private static int calculateRemovalSize(RecordStore recordStore) {
        int size = recordStore.size();
        int removalSize = (int) (size * (long) FORCED_EVICTION_PERCENTAGE / ONE_HUNDRED_PERCENT);
        return Math.max(removalSize, MIN_FORCED_EVICTION_ENTRY_REMOVE_COUNT);
    }
}
