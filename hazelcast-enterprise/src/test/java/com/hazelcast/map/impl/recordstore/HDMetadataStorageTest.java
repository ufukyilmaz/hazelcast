package com.hazelcast.map.impl.recordstore;

import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.internal.hidensity.HiDensityStorageInfo;
import com.hazelcast.internal.hidensity.impl.HDJsonMetadataRecordProcessor;
import com.hazelcast.internal.memory.StandardMemoryManager;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.serialization.impl.EnterpriseSerializationServiceBuilder;
import com.hazelcast.map.impl.record.HDJsonMetadataRecord;
import com.hazelcast.map.impl.record.HDJsonMetadataRecordAccessor;
import com.hazelcast.map.impl.record.HDJsonMetadataRecordFactory;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.query.impl.Metadata;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class HDMetadataStorageTest {

    private EnterpriseSerializationService ess;
    private HDJsonMetadataStorageImpl storage;
    private HiDensityStorageInfo storageInfo;
    private HDJsonMetadataRecordAccessor recordAccessor;
    private HDJsonMetadataRecordProcessor recordProcessor;
    private StandardMemoryManager memoryManager;

    @Before
    public void setup() {
        MemorySize memorySize = new MemorySize(100, MemoryUnit.MEGABYTES);
        memoryManager = new StandardMemoryManager(memorySize);

        ess = new EnterpriseSerializationServiceBuilder().setMemoryManager(memoryManager).build();
        storageInfo = new HiDensityStorageInfo("myStorage");
        storage = new HDJsonMetadataStorageImpl(ess, storageInfo);

        recordAccessor = new HDJsonMetadataRecordAccessor(ess);
        recordProcessor =
            new HDJsonMetadataRecordProcessor(ess, recordAccessor, storageInfo);
    }

    @Test
    public void testMetadataRecordFactory() {

        HDJsonMetadataRecordFactory recordFactory = new HDJsonMetadataRecordFactory(recordProcessor);

        for (int i = 0; i < 10; ++i) {
            HDJsonMetadataRecord record = recordFactory.newRecord(newMetadata(i));
            assertEquals(16, record.getSize());
            assertEquals("keyMetadata" + i, ess.toObject(record.getKey()));
            assertEquals("valueMetadata" + i, ess.toObject(record.getValue()));
        }

        for (int i = 0; i < 10; ++i) {
            HDJsonMetadataRecord record = recordFactory.newRecord("keyMetadata" + i, true);
            assertEquals(16, record.getSize());
            assertEquals("keyMetadata" + i, ess.toObject(record.getKey()));
            assertNull(ess.toObject(record.getValue()));
        }

        for (int i = 0; i < 10; ++i) {
            HDJsonMetadataRecord record = recordFactory.newRecord("valueMetadata" + i, false);
            assertEquals(16, record.getSize());
            assertEquals("valueMetadata" + i, ess.toObject(record.getValue()));
            assertNull(ess.toObject(record.getKey()));
        }
    }

    @Test
    public void testGet() {
        for (int i = 0; i < 100; ++i) {
            Data dataKey = ess.toData("key" + i);
            storage.set(dataKey, newMetadata(i));
            HDJsonMetadataRecord record = (HDJsonMetadataRecord) storage.get(dataKey);
            assertNotNull(record);
            assertEquals("valueMetadata" + i, record.getValueMetadata());
            assertEquals("keyMetadata" + i, record.getKeyMetadata());
        }
    }

    @Test
    public void testSet() {
        long usedNative = memoryManager.getMemoryStats().getUsedNative();

        Data dataKey = ess.toData("key");

        storage.set(dataKey, newMetadata(0));

        HDJsonMetadataRecord record = (HDJsonMetadataRecord) storage.get(dataKey);

        assertNotNull(record);
        assertEquals("valueMetadata0", record.getValueMetadata());
        assertEquals("keyMetadata0", record.getKeyMetadata());

        storage.set(dataKey, newMetadata(1));
        record = (HDJsonMetadataRecord) storage.get(dataKey);
        assertNotNull(record);
        assertEquals("valueMetadata1", record.getValueMetadata());
        assertEquals("keyMetadata1", record.getKeyMetadata());

        storage.setKey(dataKey, "keyMetadata2");
        record = (HDJsonMetadataRecord) storage.get(dataKey);
        assertNotNull(record);
        assertEquals("valueMetadata1", record.getValueMetadata());
        assertEquals("keyMetadata2", record.getKeyMetadata());

        storage.setValue(dataKey, "valueMetadata3");
        record = (HDJsonMetadataRecord) storage.get(dataKey);
        assertNotNull(record);
        assertEquals("valueMetadata3", record.getValueMetadata());
        assertEquals("keyMetadata2", record.getKeyMetadata());

        storage.setValue(dataKey, null);
        record = (HDJsonMetadataRecord) storage.get(dataKey);
        assertNotNull(record);
        assertEquals(null, record.getValueMetadata());
        assertEquals("keyMetadata2", record.getKeyMetadata());

        storage.setKey(dataKey, null);
        record = (HDJsonMetadataRecord) storage.get(dataKey);
        assertNull(record);

        assertEquals(usedNative, memoryManager.getMemoryStats().getUsedNative());
    }

    @Test
    public void testRemove() {
        long usedNative = memoryManager.getMemoryStats().getUsedNative();

        Data dataKey = ess.toData("key");
        storage.set(dataKey, newMetadata(0));

        HDJsonMetadataRecord record = (HDJsonMetadataRecord) storage.get(dataKey);
        assertNotNull(record);

        storage.remove(dataKey);
        assertNull(storage.get(dataKey));

        assertEquals(usedNative, memoryManager.getMemoryStats().getUsedNative());
    }

    @Test
    public void testRemoveByMetadataKeyAndValue() {
        long usedNative = memoryManager.getMemoryStats().getUsedNative();

        Data dataKey = ess.toData("key");
        storage.set(dataKey, newMetadata(0));

        HDJsonMetadataRecord record = (HDJsonMetadataRecord) storage.get(dataKey);
        assertNotNull(record);

        storage.setKey(dataKey, "metadataKey2");
        assertNotNull(storage.get(dataKey));

        storage.setKey(dataKey, null);
        assertNotNull(storage.get(dataKey));

        storage.setValue(dataKey, "metadataValue2");
        assertNotNull(storage.get(dataKey));

        storage.setValue(dataKey, null);
        assertNull(storage.get(dataKey));

        assertEquals(usedNative, memoryManager.getMemoryStats().getUsedNative());
    }

    @Test
    public void testRemoveReplacedMetadata() {
        long usedNative = memoryManager.getMemoryStats().getUsedNative();

        Data dataKey = ess.toData("key");
        storage.set(dataKey, newMetadata(0));

        HDJsonMetadataRecord record = (HDJsonMetadataRecord) storage.get(dataKey);
        assertNotNull(record);

        storage.set(dataKey, newMetadata(1));
        assertNotNull(storage.get(dataKey));

        storage.remove(dataKey);
        assertNull(storage.get(dataKey));

        assertEquals(usedNative, memoryManager.getMemoryStats().getUsedNative());
    }

    @Test
    public void testClear() {

        for (int i = 0; i < 100; ++i) {
            Data dataKey = ess.toData("key" + i);
            storage.set(dataKey, newMetadata(i));
            assertNotNull(storage.get(dataKey));
        }

        storage.clear();

        for (int i = 0; i < 100; ++i) {
            Data dataKey = ess.toData("key" + i);
            assertNull(storage.get(dataKey));
        }

        // 4096 bytes are taken by the open addressing array
        assertEquals(4096, memoryManager.getMemoryStats().getUsedNative());
    }

    private Metadata newMetadata(int index) {
        Metadata metadata = new Metadata("keyMetadata" + index, "valueMetadata" + index);
        return metadata;
    }
}
