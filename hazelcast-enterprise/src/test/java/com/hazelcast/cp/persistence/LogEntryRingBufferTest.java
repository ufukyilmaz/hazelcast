package com.hazelcast.cp.persistence;

import com.hazelcast.cp.internal.raft.impl.persistence.LogFileStructure;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class LogEntryRingBufferTest {

    @Rule
    public final ExpectedException exceptionRule = ExpectedException.none();

    private LogEntryRingBuffer buffer;

    @Test
    public void when_bufferEmpty_then_topIndexIsBottomIndexMinusOne() {
        // Given
        long bottomIndex = 43;

        // When
        buffer = new LogEntryRingBuffer(10, bottomIndex);

        // Then
        assertEquals(bottomIndex - 1, buffer.topIndex());
    }

    @Test
    public void when_bufferEmpty_then_getEntryOffsetFails() {
        // Given
        long bottomIndex = 43;

        // When
        buffer = new LogEntryRingBuffer(10, bottomIndex);

        // Then
        exceptionRule.expect(IndexOutOfBoundsException.class);
        buffer.getEntryOffset(bottomIndex);
    }

    @Test
    public void when_addOneOffset_then_topIndexIsBottomIndex() {
        // Given
        long bottomIndex = 43;
        buffer = new LogEntryRingBuffer(10, bottomIndex);

        // When
        buffer.addEntryOffset(100);

        // Then
        assertEquals(bottomIndex, buffer.topIndex());
    }

    @Test
    public void when_addOneOffset_then_getItAtBottomIndex() {
        // Given
        long bottomIndex = 43;
        buffer = new LogEntryRingBuffer(10, bottomIndex);

        // When
        buffer.addEntryOffset(100);

        // Then
        assertEquals(100, buffer.getEntryOffset(bottomIndex));
    }

    @Test
    public void when_addOneOffset_then_cantGetBottomIndexPlusOne() {
        // Given
        long bottomIndex = 43;
        buffer = new LogEntryRingBuffer(10, bottomIndex);

        // When
        buffer.addEntryOffset(100);

        // Then
        exceptionRule.expect(IndexOutOfBoundsException.class);
        buffer.getEntryOffset(bottomIndex + 1);
    }

    @Test
    public void when_addManyEntries_then_getThemAtCorrectIndex() {
        long bottomIndex = 43;
        buffer = new LogEntryRingBuffer(3, bottomIndex);

        buffer.addEntryOffset(100);
        assertEquals(100, buffer.getEntryOffset(bottomIndex));
        assertEquals(bottomIndex, buffer.topIndex());

        buffer.addEntryOffset(200);
        assertEquals(200, buffer.getEntryOffset(bottomIndex + 1));
        assertEquals(bottomIndex + 1, buffer.topIndex());

        buffer.addEntryOffset(300);
        assertEquals(300, buffer.getEntryOffset(bottomIndex + 2));
        assertEquals(bottomIndex + 2, buffer.topIndex());

        buffer.addEntryOffset(400);
        assertEquals(400, buffer.getEntryOffset(bottomIndex + 3));
        assertEquals(bottomIndex + 3, buffer.topIndex());

        buffer.addEntryOffset(500);
        assertEquals(500, buffer.getEntryOffset(bottomIndex + 4));
        assertEquals(bottomIndex + 4, buffer.topIndex());
    }

    @Test
    public void when_deleteEntriesFromIndex_then_thatIndexUnavailable() {
        // Given
        long bottomIndex = 43;
        long deletionStartIndex = bottomIndex + 1;
        buffer = new LogEntryRingBuffer(7, bottomIndex);
        buffer.addEntryOffset(100);
        buffer.addEntryOffset(200);
        buffer.addEntryOffset(300);

        // When
        buffer.deleteEntriesFrom(deletionStartIndex);

        // Then
        exceptionRule.expect(IndexOutOfBoundsException.class);
        buffer.getEntryOffset(deletionStartIndex);
    }

    @Test
    public void when_deleteEntriesFromIndex_then_earlierEntriesRemain() {
        // Given
        long bottomIndex = 43;
        buffer = new LogEntryRingBuffer(7, bottomIndex);
        buffer.addEntryOffset(100);
        buffer.addEntryOffset(200);
        buffer.addEntryOffset(300);
        buffer.addEntryOffset(400);

        // When
        long offset = buffer.deleteEntriesFrom(bottomIndex + 2);

        // Then
        assertEquals(300, offset);
        assertEquals(100, buffer.getEntryOffset(bottomIndex));
        assertEquals(200, buffer.getEntryOffset(bottomIndex + 1));
    }

    @Test
    public void when_deleteEntriesFromIndex_then_topIndexAdjusted() {
        // Given
        long bottomIndex = 43;
        buffer = new LogEntryRingBuffer(7, bottomIndex);
        buffer.addEntryOffset(100);
        buffer.addEntryOffset(200);
        buffer.addEntryOffset(300);

        // When
        buffer.deleteEntriesFrom(bottomIndex + 1);

        // Then
        assertEquals(bottomIndex, buffer.topIndex());
    }

    @Test
    public void when_deleteEntriesOnEmptyBuffer_then_fail() {
        // Given
        long bottomIndex = 43;
        buffer = new LogEntryRingBuffer(7, bottomIndex);

        // When-Then
        exceptionRule.expect(IndexOutOfBoundsException.class);
        buffer.deleteEntriesFrom(bottomIndex);
    }

    @Test
    public void when_deleteEntriesFromLessThanBottomIndex_then_fail() {
        // Given
        long bottomIndex = 43;
        buffer = new LogEntryRingBuffer(7, bottomIndex);
        buffer.addEntryOffset(100);

        // When-Then
        exceptionRule.expect(IndexOutOfBoundsException.class);
        buffer.deleteEntriesFrom(bottomIndex - 1);
    }

    @Test
    public void test_deleteEntriesFromAboveTopIndex_then_fail() {
        // Given
        long bottomIndex = 43;
        buffer = new LogEntryRingBuffer(7, bottomIndex);
        buffer.addEntryOffset(100);

        // When-Then
        exceptionRule.expect(IndexOutOfBoundsException.class);
        buffer.deleteEntriesFrom(buffer.topIndex() + 1);
    }

    @Test
    public void test_addBeyondCapacityAndDeleteEntries() {
        // Given
        long bottomIndex = 43;
        long deletionStartIndex = bottomIndex + 4;
        buffer = new LogEntryRingBuffer(4, bottomIndex);
        buffer.addEntryOffset(100);
        buffer.addEntryOffset(200);
        buffer.addEntryOffset(300);
        buffer.addEntryOffset(400);
        buffer.addEntryOffset(500);
        buffer.addEntryOffset(600);

        // When
        buffer.deleteEntriesFrom(deletionStartIndex);

        // Then
        assertEquals(deletionStartIndex - 1, buffer.topIndex());
        assertEquals(400, buffer.getEntryOffset(deletionStartIndex - 1));
    }

    @Test
    public void when_adjustToNewFile_then_offsetsChange() {
        // Given
        long bottomIndex = 43;
        buffer = new LogEntryRingBuffer(7, bottomIndex);
        buffer.addEntryOffset(100);
        buffer.addEntryOffset(200);
        buffer.addEntryOffset(300);
        buffer.addEntryOffset(400);
        buffer.addEntryOffset(500);
        buffer.addEntryOffset(600);
        long snapshotIndex = bottomIndex + 2;
        int newStartOffset = 25;

        // When
        buffer.adjustToNewFile(newStartOffset, snapshotIndex);

        // Then
        assertEquals(newStartOffset, buffer.getEntryOffset(snapshotIndex + 1));
        assertEquals(125, buffer.getEntryOffset(snapshotIndex + 2));
        assertEquals(225, buffer.getEntryOffset(snapshotIndex + 3));
    }

    @Test
    public void when_adjustToNewFileWithIndexLessThanBottom_then_fail() {
        // Given
        long bottomIndex = 43;
        buffer = new LogEntryRingBuffer(7, bottomIndex);
        buffer.addEntryOffset(100);

        // When-Then
        exceptionRule.expect(IndexOutOfBoundsException.class);
        buffer.adjustToNewFile(500, bottomIndex - 1);
    }

    @Test
    public void when_adjustToNewFile_then_cantAdjustWithLowerSnapshotIndex() {
        // Given
        long bottomIndex = 43;
        buffer = new LogEntryRingBuffer(7, bottomIndex);
        buffer.addEntryOffset(100);
        buffer.addEntryOffset(200);

        // When
        buffer.adjustToNewFile(500, bottomIndex);

        // Then
        assertEquals(bottomIndex + 1, buffer.topIndex());
        exceptionRule.expect(IndexOutOfBoundsException.class);
        buffer.adjustToNewFile(500, bottomIndex);
    }

    @Test
    public void when_adjustToNewFile_then_topIndexReportedCorrectly() {
        // Given
        long bottomIndex = 43;
        buffer = new LogEntryRingBuffer(7, bottomIndex);
        buffer.addEntryOffset(100);
        buffer.addEntryOffset(200);

        // When
        buffer.adjustToNewFile(500, bottomIndex);

        // Then
        assertEquals(bottomIndex + 1, buffer.topIndex());
        exceptionRule.expect(IndexOutOfBoundsException.class);
        buffer.getEntryOffset(bottomIndex + 2);
    }

    @Test
    public void when_adjustToNewFileWithIndexBeyondRange_then_bufferEmpty() {
        // Given
        long bottomIndex = 43;
        buffer = new LogEntryRingBuffer(7, bottomIndex);
        buffer.addEntryOffset(100);

        // When
        buffer.adjustToNewFile(500, bottomIndex + 10);

        // Then
        exceptionRule.expect(IndexOutOfBoundsException.class);
        buffer.getEntryOffset(bottomIndex + 10);
    }

    @Test
    public void when_exportEntryOffsetsOfPartiallyFullBuffer_then_allOffsetsCorrect() {
        buffer = new LogEntryRingBuffer(4, 1);
        buffer.addEntryOffset(100);
        buffer.addEntryOffset(200);

        // When
        long[] offsets = buffer.exportEntryOffsets();

        // Then
        assertEquals(offsets.length, 2);
        assertEquals(100, offsets[0]);
        assertEquals(200, offsets[1]);
    }

    @Test
    public void when_exportEntryOffsetsOfFullBuffer_then_allOffsetsCorrect() {
        buffer = new LogEntryRingBuffer(4, 1);
        buffer.addEntryOffset(100);
        buffer.addEntryOffset(200);
        buffer.addEntryOffset(300);
        buffer.addEntryOffset(400);

        // When
        long[] offsets = buffer.exportEntryOffsets();

        // Then
        assertEquals(100, offsets[0]);
        assertEquals(200, offsets[1]);
        assertEquals(300, offsets[2]);
        assertEquals(400, offsets[3]);
    }

    @Test
    public void when_exportEntryOffsetsAfterEvictingEldestEntries_then_allOffsetsCorrect() {
        buffer = new LogEntryRingBuffer(4, 1);
        buffer.addEntryOffset(100);
        buffer.addEntryOffset(200);
        buffer.addEntryOffset(300);
        buffer.addEntryOffset(400);
        buffer.addEntryOffset(500);
        buffer.addEntryOffset(600);

        // When
        long[] offsets = buffer.exportEntryOffsets();

        // Then
        assertEquals(300, offsets[0]);
        assertEquals(400, offsets[1]);
        assertEquals(500, offsets[2]);
        assertEquals(600, offsets[3]);
    }

    @Test
    public void when_importEntriesToFillBuffer_then_allOffsetsCorrect() {
        long[] offsets = {100, 200, 300, 400};
        buffer = new LogEntryRingBuffer(4, new LogFileStructure("", offsets, 1));

        assertEquals(100, buffer.getEntryOffset(1));
        assertEquals(200, buffer.getEntryOffset(2));
        assertEquals(300, buffer.getEntryOffset(3));
        assertEquals(400, buffer.getEntryOffset(4));
    }

    @Test
    public void when_importMoreEntriesThanCapacity_then_fail() {
        long[] offsets = {100, 200, 300, 400};

        exceptionRule.expect(IllegalArgumentException.class);
        new LogEntryRingBuffer(3, new LogFileStructure("", offsets, 1));
    }

}
