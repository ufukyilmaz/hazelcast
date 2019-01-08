package com.hazelcast.map.impl.wan;

import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStoreMutationObserver;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;
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

    @Mock
    private SerializationService serializationServiceMock;

    private RecordStoreMutationObserver<Record> observer;

    @Before
    public void setUp() {
        initMocks(this);
        observer = new MerkleTreeUpdaterRecordStoreMutationObserver<Record>(merkleTreeMock, serializationServiceMock);
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
        String value = "42";
        Data valueAsData = new WrappedData(value);
        when(serializationServiceMock.toData(value)).thenReturn(valueAsData);
        Record record = mock(Record.class);
        when(record.getValue()).thenReturn(value);

        observer.onPutRecord(key, record);

        verify(merkleTreeMock).updateAdd(eq(key), eq(valueAsData));
    }

    @Test
    public void onReplicationPutRecord() {
        Data key = mock(Data.class);
        String value = "42";
        Data valueAsData = new WrappedData(value);
        when(serializationServiceMock.toData(value)).thenReturn(valueAsData);
        Record record = mock(Record.class);
        when(record.getValue()).thenReturn(value);

        observer.onReplicationPutRecord(key, record);

        verify(merkleTreeMock).updateAdd(eq(key), eq(valueAsData));
    }

    @Test
    public void onUpdateRecord() {
        Data key = mock(Data.class);
        String value = "42";
        String newValue = "42x";
        Data valueAsData = new WrappedData(value);
        Data newValueAsData = new WrappedData(newValue);
        when(serializationServiceMock.toData(value)).thenReturn(valueAsData);
        when(serializationServiceMock.toData(newValue)).thenReturn(newValueAsData);
        Record record = mock(Record.class);
        when(record.getValue()).thenReturn(value);

        observer.onUpdateRecord(key, record, newValue);

        verify(merkleTreeMock).updateReplace(eq(key), eq(valueAsData), eq(newValueAsData));
    }

    @Test
    public void onRemoveRecord() {
        Data key = mock(Data.class);
        String value = "42";
        Data valueAsData = new WrappedData(value);
        when(serializationServiceMock.toData(value)).thenReturn(valueAsData);
        Record record = mock(Record.class);
        when(record.getValue()).thenReturn(value);

        observer.onRemoveRecord(key, record);

        verify(merkleTreeMock).updateRemove(eq(key), eq(valueAsData));
    }

    @Test
    public void onEvictRecord() {
        Data key = mock(Data.class);
        String value = "42";
        Data valueAsData = new WrappedData(value);
        when(serializationServiceMock.toData(value)).thenReturn(valueAsData);
        Record record = mock(Record.class);
        when(record.getValue()).thenReturn(value);

        observer.onEvictRecord(key, record);

        verify(merkleTreeMock).updateRemove(eq(key), eq(valueAsData));
    }

    private static class WrappedData implements Data {

        private final Object wrapped;

        private WrappedData(Object wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public byte[] toByteArray() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getType() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int totalSize() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void copyTo(byte[] dest, int destPos) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int dataSize() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getHeapCost() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getPartitionHash() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasPartitionHash() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long hash64() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isPortable() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isJson() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int hashCode() {
            return wrapped.hashCode();
        }
    }
}
