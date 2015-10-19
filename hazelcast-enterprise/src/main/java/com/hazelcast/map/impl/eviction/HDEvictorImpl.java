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
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.Clock;

import java.util.Arrays;
import java.util.Iterator;

/**
 * {@link Evictor} for maps which has {@link com.hazelcast.config.InMemoryFormat#BINARY BINARY} in-memory-format.
 */
public class HDEvictorImpl extends EvictorImpl {

    public HDEvictorImpl(EvictionChecker evictionChecker, MapServiceContext mapServiceContext) {
        super(evictionChecker, mapServiceContext);
    }

    @Override
    public void removeSize(int removalSize, RecordStore recordStore) {
        long now = Clock.currentTimeMillis();
        MapConfig mapConfig = recordStore.getMapContainer().getMapConfig();

        boolean backup = isBackup(recordStore);

        final EvictionPolicy evictionPolicy = mapConfig.getEvictionPolicy();
        // criteria is a long value, like last access times or hits,
        // used for calculating LFU or LRU.
        final long[] criterias = createAndPopulateEvictionCriteriaArray(recordStore, evictionPolicy);
        if (criterias == null) {
            return;
        }
        Arrays.sort(criterias);
        // check in case record store size may be smaller than evictable size.
        final int evictableBaseIndex = getEvictionStartIndex(criterias, removalSize);
        final long criteriaValue = criterias[evictableBaseIndex];
        int evictedRecordCounter = 0;
        final Iterator<Record> iterator = recordStore.getStorage().values().iterator();
        while (iterator.hasNext()) {
            Record record = iterator.next();
            Data key = record.getKey();
            long value = getEvictionCriteriaValue(record, evictionPolicy);
            if (value <= criteriaValue) {
                if (tryEvict(key, record, recordStore, backup, now)) {
                    iterator.remove();
                    evictedRecordCounter++;
                }
            }
            if (evictedRecordCounter >= removalSize) {
                break;
            }
        }
    }

    @Override
    protected boolean tryEvict(Data key, Record record, RecordStore recordStore, boolean backup, long now) {
        Object value = record.getValue();

        if (recordStore.isLocked(key)) {
            return false;
        }

        if (!backup) {
            boolean expired = recordStore.isExpired(record, now, false);
            recordStore.doPostEvictionOperations(key, value, expired);
        }

        return true;
    }

    @Override
    public int findRemovalSize(RecordStore recordStore) {
        MapConfig mapConfig = recordStore.getMapContainer().getMapConfig();
        MaxSizeConfig.MaxSizePolicy maxSizePolicy = mapConfig.getMaxSizeConfig().getMaxSizePolicy();
        int evictionPercentage = mapConfig.getEvictionPercentage();

        switch (maxSizePolicy) {
            case USED_NATIVE_MEMORY_PERCENTAGE:
            case USED_NATIVE_MEMORY_SIZE:
            case FREE_NATIVE_MEMORY_PERCENTAGE:
            case FREE_NATIVE_MEMORY_SIZE:
                // if we have an evictable size, be sure to evict at least one entry in worst case.
                return Math.max(recordStore.size() * evictionPercentage / ONE_HUNDRED_PERCENT, 1);
            default:
                return super.findRemovalSize(recordStore);
        }
    }
}
