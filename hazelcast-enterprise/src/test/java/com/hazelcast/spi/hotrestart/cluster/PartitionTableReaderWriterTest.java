package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.PartitionTableView;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.FileNotFoundException;
import java.io.IOException;

import static com.hazelcast.internal.partition.InternalPartition.MAX_REPLICA_COUNT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class PartitionTableReaderWriterTest extends MetadataReaderWriterTestBase {

    @Test
    public void test_readNotExistingFolder() throws IOException {
        PartitionTableReader reader = new PartitionTableReader(getNonExistingFolder(), 100);
        reader.read();
        assertPartitionTableEmpty(reader.getTable());
    }

    @Test
    public void test_readEmptyFolder() throws IOException {
        PartitionTableReader reader = new PartitionTableReader(folder, 100);
        reader.read();
        PartitionTableView table = reader.getTable();
        assertEquals(0, table.getVersion());
        assertPartitionTableEmpty(table);
    }

    private void assertPartitionTableEmpty(PartitionTableView table) {
        for (int i = 0; i < table.getLength(); i++) {
            for (int j = 0; j < MAX_REPLICA_COUNT; j++) {
                assertNull(table.getAddress(i, j));
            }
        }
    }

    @Test(expected = FileNotFoundException.class)
    public void test_writeNotExistingFolder() throws IOException {
        PartitionTableView partitions = new PartitionTableView(new Address[100][InternalPartition.MAX_REPLICA_COUNT], 0);
        PartitionTableWriter writer = new PartitionTableWriter(getNonExistingFolder());
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
        PartitionTableView expectedPartitionTable = initializePartitionTable(members, partitionCount);

        PartitionTableWriter writer = new PartitionTableWriter(folder);
        writer.write(expectedPartitionTable);

        PartitionTableReader reader = new PartitionTableReader(folder, partitionCount);
        reader.read();

        PartitionTableView partitionTable = reader.getTable();
        assertEquals(expectedPartitionTable, partitionTable);
    }

    @Test
    public void test_withIncreasingPartitionCount() throws IOException {
        Address[] members = initializeAddresses(0);
        int partitionCount = 100;

        PartitionTableView expectedPartitionTable = initializePartitionTable(members, partitionCount);

        PartitionTableWriter writer = new PartitionTableWriter(folder);
        writer.write(expectedPartitionTable);

        int newPartitionCount = partitionCount + 1;
        PartitionTableReader reader = new PartitionTableReader(folder, newPartitionCount);

        try {
            reader.read();
            fail("Should fail to read partition table!");
        } catch (IOException expected) {
        }
    }

    @Test
    public void test_withDecreasingPartitionCount() throws IOException {
        Address[] members = initializeAddresses(0);
        int partitionCount = 100;

        PartitionTableView expectedPartitionTable = initializePartitionTable(members, partitionCount);

        PartitionTableWriter writer = new PartitionTableWriter(folder);
        writer.write(expectedPartitionTable);

        int newPartitionCount = partitionCount - 1;
        PartitionTableReader reader = new PartitionTableReader(folder, newPartitionCount);

        try {
            reader.read();
            fail("Should fail to read partition table!");
        } catch (IOException expected) {
        }
    }

    public PartitionTableView initializePartitionTable(Address[] members, int partitionCount) {
        Address[][] addresses = new Address[partitionCount][MAX_REPLICA_COUNT];

        int replicaCount = Math.min(members.length, MAX_REPLICA_COUNT);

        for (int i = 0; i < partitionCount; i++) {
            Address[] replicas = addresses[i];
            for (int j = 0; j < replicaCount; j++) {
                replicas[j] = members[(i + j) % members.length];
            }
        }
        return new PartitionTableView(addresses, partitionCount * replicaCount);
    }
}
