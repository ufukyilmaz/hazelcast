package com.hazelcast.cache.impl.hidensity.operation;

import com.hazelcast.internal.serialization.Data;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.cache.impl.record.CacheRecord.TIME_NOT_AVAILABLE;

/**
 * Holds multiple cache backup records by their keys and values.
 */
class CacheBackupRecordStore {

    final List<CacheBackupRecord> backupRecords;

    CacheBackupRecordStore() {
        backupRecords = new ArrayList<CacheBackupRecord>();
    }

    CacheBackupRecordStore(int size) {
        backupRecords = new ArrayList<CacheBackupRecord>(size);
    }

    void addBackupRecord(Data key, Data value) {
        backupRecords.add(new CacheBackupRecord(key, value, TIME_NOT_AVAILABLE));
    }

    void addBackupRecord(Data key, Data value, long creationTime) {
        backupRecords.add(new CacheBackupRecord(key, value, creationTime));
    }

    boolean isEmpty() {
        return backupRecords.isEmpty();
    }

    /**
     * Represents a cache backup record by wrapping key and value of it.
     */
    static class CacheBackupRecord {

        final Data key;
        final Data value;
        final long creationTime;

        CacheBackupRecord(Data key, Data value, long creationTime) {
            this.key = key;
            this.value = value;
            this.creationTime = creationTime;
        }

    }

}
