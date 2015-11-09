package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.nio.Address;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.FileNotFoundException;
import java.io.IOException;

import static com.hazelcast.partition.InternalPartition.MAX_REPLICA_COUNT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class PartitionTableReaderWriterTest extends AbstractReaderWriterTest {

    @Test
    public void test_readNotExistingFolder() throws IOException {
        PartitionTableReader reader = new PartitionTableReader(getNotExistingFolder(), 100);
        reader.read();
        assertPartitionTableEmpty(reader.getTable());
    }

    @Test
    public void test_readEmptyFolder() throws IOException {
        PartitionTableReader reader = new PartitionTableReader(folder, 100);
        reader.read();
        assertPartitionTableEmpty(reader.getTable());
    }

    private void assertPartitionTableEmpty(Address[][] table) {
        for (Address[] replicas : table) {
            for (int j = 0; j < MAX_REPLICA_COUNT; j++) {
                assertNull(replicas[j]);
            }
        }
    }

    @Test(expected = FileNotFoundException.class)
    public void test_writeNotExistingFolder() throws IOException {
        InternalPartition[] partitions = new InternalPartition[100];
        PartitionTableWriter writer = new PartitionTableWriter(getNotExistingFolder());
        writer.write(partitions);
    }

    @Test
    public void test_EmptyWriteRead() throws IOException {
        test_WriteRead(0);
    }

    @Test
    public void test_WriteRead_fewMembers() throws IOException {
        test_WriteRead(3);
    }

    @Test
    public void test_WriteRead_manyMembers() throws IOException {
        test_WriteRead(100);
    }

    private void test_WriteRead(int memberCount) throws IOException {
        Address[] members = initializeAddresses(memberCount);

        final int partitionCount = 100;
        InternalPartition[] partitions = initializePartitionTable(members, partitionCount);

        PartitionTableWriter writer = new PartitionTableWriter(folder);
        writer.write(partitions);

        PartitionTableReader reader = new PartitionTableReader(folder, partitionCount);
        reader.read();

        Address[][] partitionTable = reader.getTable();
        assertPartitionTableEquals(partitions, partitionTable);
    }

    private void assertPartitionTableEquals(InternalPartition[] partitions, Address[][] partitionTable) {
        assertNotNull(partitionTable);
        assertEquals(partitions.length, partitionTable.length);

        for (int i = 0; i < partitions.length; i++) {
            InternalPartition partition = partitions[i];
            Address[] replicas = partitionTable[i];

            assertPartitionEquals(partition, replicas);
        }
    }

    private void assertPartitionEquals(InternalPartition partition, Address[] replicas) {
        assertNotNull(replicas);
        assertEquals(MAX_REPLICA_COUNT, replicas.length);

        for (int i = 0; i < MAX_REPLICA_COUNT; i++) {
            Address address = partition.getReplicaAddress(i);
            Address replica = replicas[i];
            assertAddressEquals(address, replica);
        }
    }

    public InternalPartition[] initializePartitionTable(Address[] members, int partitionCount) {
        InternalPartition[] partitions = new InternalPartition[partitionCount];

        int replicaCount = Math.min(members.length, MAX_REPLICA_COUNT);

        for (int i = 0; i < partitionCount; i++) {
            Address[] replicas = new Address[MAX_REPLICA_COUNT];
            for (int j = 0; j < replicaCount; j++) {
                replicas[j] = members[(i + j) % members.length];
            }
            partitions[i] = new PartitionImpl(i, replicas);
        }
        return partitions;
    }

    private static class PartitionImpl implements InternalPartition {
        final int partitionId;
        final Address[] replicas;

        private PartitionImpl(int partitionId, Address[] replicas) {
            this.partitionId = partitionId;
            this.replicas = replicas;
        }

        @Override
        public boolean isLocal() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getPartitionId() {
            return partitionId;
        }

        @Override
        public Address getOwnerOrNull() {
            return replicas[0];
        }

        @Override
        public boolean isMigrating() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Address getReplicaAddress(int replicaIndex) {
            return replicas[replicaIndex];
        }

        @Override
        public boolean isOwnerOrBackup(Address address) {
            throw new UnsupportedOperationException();
        }
    }
}
