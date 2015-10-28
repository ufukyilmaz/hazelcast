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

import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.elastic.map.SampleableElasticHashMap;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.record.HDRecord;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.HDStorageImpl;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.Clock;

import java.util.Iterator;

/**
 * {@link Evictor} for maps which has {@link com.hazelcast.config.InMemoryFormat#NATIVE NATIVE} in-memory-format.
 * <p/>
 * This evictor is sampling based evictor. So independent of the size of record-store, eviction works in constant time.
 */
public class HDEvictorImpl extends EvictorImpl {

    private static final int SAMPLE_COUNT = 15;
    private static final int MAX_EVICTED_ENTRY_COUNT_IN_ONE_ROUND = 1;
    private static final int ONE_HUNDRED_PERCENT = 100;
    private static final int FORCED_EVICTION_PERCENTAGE = 20;
    private static final int MIN_FORCED_EVICTION_ENTRY_REMOVE_COUNT = 20;

    public HDEvictorImpl(EvictionChecker evictionChecker, MapServiceContext mapServiceContext) {
        super(evictionChecker, mapServiceContext);
    }

    @Override
    public void removeSize(int removalSize, RecordStore recordStore) {
        if (!isEvictable(recordStore)) {
            return;
        }

        boolean backup = isBackup(recordStore);
        MapConfig mapConfig = recordStore.getMapContainer().getMapConfig();
        EvictionPolicy evictionPolicy = mapConfig.getEvictionPolicy();

        Iterable<SampleableElasticHashMap.SamplingEntry> samples
                = ((HDStorageImpl) recordStore.getStorage()).getRandomSamples(SAMPLE_COUNT);

        long prevCriteriaValue = -1;
        SampleableElasticHashMap.SamplingEntry entry = null;
        for (SampleableElasticHashMap.SamplingEntry sample : samples) {
            // evictor does not remove locked keys.
            if (recordStore.isLocked(sample.getKey())) {
                continue;
            }

            // This `criteriaValue` represents the criteria value for evictions like LFU, LRU.
            // By using this value we are trying to select most appropriate entry to evict.
            long criteriaValue = getEvictionCriteriaValue((HDRecord) sample.getValue(), evictionPolicy);
            if (prevCriteriaValue == -1) {
                prevCriteriaValue = criteriaValue;
                entry = sample;
            } else if (criteriaValue < prevCriteriaValue) {
                prevCriteriaValue = criteriaValue;
                entry = sample;
            }
        }

        if (entry != null) {
            Record record = (HDRecord) entry.getValue();
            fireEvent(record, recordStore, backup, getNow());

            recordStore.evict(record.getKey(), backup);
        }
    }

    protected static void fireEvent(Record record, RecordStore recordStore, boolean backup, long now) {
        if (!backup) {
            boolean expired = recordStore.isExpired(record, now, false);
            recordStore.doPostEvictionOperations(record.getKey(), record.getValue(), expired);
        }
    }

    public void forceEvict(RecordStore recordStore) {
        if (!isEvictable(recordStore)) {
            return;
        }

        long now = getNow();
        boolean backup = isBackup(recordStore);

        int removalSize = calculateRemovalSize(recordStore);
        int removedEntryCount = 0;
        Iterator<Record> iterator = recordStore.getStorage().values().iterator();
        while (iterator.hasNext()) {
            Record record = iterator.next();
            Data key = record.getKey();
            if (!recordStore.isLocked(key)) {
                fireEvent(record, recordStore, backup, now);
                iterator.remove();
                removedEntryCount++;
            }

            if (removedEntryCount >= removalSize) {
                break;
            }
        }
    }

    private static boolean isEvictable(RecordStore recordStore) {
        return recordStore.isEvictionEnabled() && recordStore.size() > 0;
    }

    private static int calculateRemovalSize(RecordStore recordStore) {
        int size = recordStore.size();
        int removalSize = (int) (size * (long) FORCED_EVICTION_PERCENTAGE / ONE_HUNDRED_PERCENT);
        return Math.max(removalSize, MIN_FORCED_EVICTION_ENTRY_REMOVE_COUNT);
    }

    private static long getNow() {
        return Clock.currentTimeMillis();
    }

    @Override
    public int findRemovalSize(RecordStore recordStore) {
        return MAX_EVICTED_ENTRY_COUNT_IN_ONE_ROUND;
    }
}
