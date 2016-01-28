package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.nio.serialization.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * Holds multiple cache backup record by their keys and values.
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
        backupRecords.add(new CacheBackupRecord(key, value));
    }

    boolean isEmpty() {
        return backupRecords.isEmpty();
    }

    /**
     * Represents cache backup record by wrapping key and value of it.
     */
    static class CacheBackupRecord {

        final Data key;
        final Data value;

        CacheBackupRecord(Data key, Data value) {
            this.key = key;
            this.value = value;
        }

    }

}
