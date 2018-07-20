package com.hazelcast.map.impl.wan;

import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStoreMutationObserver;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.merkletree.MerkleTree;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MerkleTreeUpdaterRecordStoreMutationObserverTest {

    @Mock
    private MerkleTree merkleTreeMock;

    private RecordStoreMutationObserver<Record> observer;

    @Before
    public void setUp() {
        initMocks(this);
        observer = new MerkleTreeUpdaterRecordStoreMutationObserver<Record>(merkleTreeMock);
    }

    @Test
    public void onClear() {
        observer.onClear();
        verify(merkleTreeMock).clear();
    }

    @Test
    public void onReset() {
        observer.onReset();
        verify(merkleTreeMock).clear();
    }

    @Test
    public void onDestroy() {
        observer.onDestroy(false);
        verify(merkleTreeMock).clear();
    }

    @Test
    public void onDestroyInternal() {
        observer.onDestroy(true);
        verify(merkleTreeMock).clear();
    }

    @Test
    public void onPutRecord() {
        Data key = mock(Data.class);
        Record record = mock(Record.class);
        String value = "42";
        when(record.getValue()).thenReturn(value);

        observer.onPutRecord(key, record);

        verify(merkleTreeMock).updateAdd(eq(key), eq(value));
    }

    @Test
    public void onReplicationPutRecord() {
        Data key = mock(Data.class);
        Record record = mock(Record.class);
        String value = "42";
        when(record.getValue()).thenReturn(value);

        observer.onReplicationPutRecord(key, record);

        verify(merkleTreeMock).updateAdd(eq(key), eq(value));
    }

    @Test
    public void onUpdateRecord() {
        Data key = mock(Data.class);
        Record record = mock(Record.class);
        String value = "42";
        String newValue = "42x";
        when(record.getValue()).thenReturn(value);

        observer.onUpdateRecord(key, record, newValue);

        verify(merkleTreeMock).updateReplace(eq(key), eq(value), eq(newValue));
    }

    @Test
    public void onRemoveRecord() {
        Data key = mock(Data.class);
        Record record = mock(Record.class);
        String value = "42";
        when(record.getValue()).thenReturn(value);

        observer.onRemoveRecord(key, record);

        verify(merkleTreeMock).updateRemove(eq(key), eq(value));
    }

    @Test
    public void onEvictRecord() {
        Data key = mock(Data.class);
        Record record = mock(Record.class);
        String value = "42";
        when(record.getValue()).thenReturn(value);

        observer.onEvictRecord(key, record);

        verify(merkleTreeMock).updateRemove(eq(key), eq(value));
    }
}
