package com.hazelcast.spi.hotrestart;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TestUtils {

    public static class TestRecord {
        public final long recordSeq;
        public final long keyPrefix;
        public final byte[] keyBytes;
        public final byte[] valueBytes;

        public TestRecord(AtomicInteger counter) {
            this.keyPrefix = counter.incrementAndGet();
            this.recordSeq = counter.incrementAndGet();
            this.keyBytes = new byte[8];
            this.valueBytes = new byte[8];
            for (int i = 0; i < 8; i++) {
                this.keyBytes[i] = (byte) counter.incrementAndGet();
                this.keyBytes[i] = (byte) counter.incrementAndGet();
            }
        }
    }

    public static void assertRecordEquals(String msg, TestRecord expected, DataInputStream actual, boolean valueChunk) throws IOException {
        assertEquals(msg, expected.recordSeq, actual.readLong());
        assertEquals(msg, expected.keyPrefix, actual.readLong());
        assertEquals(msg, expected.keyBytes.length, actual.readInt());
        if (valueChunk) {
            assertEquals(msg, expected.valueBytes.length, actual.readInt());
        }

        byte[] actualKeyBytes = new byte[expected.keyBytes.length];
        actual.read(actualKeyBytes, 0, expected.keyBytes.length);
        assertArrayEquals(msg, expected.keyBytes, actualKeyBytes);

        if (valueChunk) {
            byte[] actualValueBytes = new byte[expected.valueBytes.length];
            actual.read(actualValueBytes, 0, expected.valueBytes.length);
            assertArrayEquals(msg, expected.valueBytes, actualValueBytes);
        }
    }

    public static void assertRecordEquals(TestRecord expected, DataInputStream actual, boolean valueChunk) throws IOException {
        assertRecordEquals("", expected, actual, valueChunk);
    }

    public static File temporaryFile() throws IOException {
        File file = File.createTempFile(UUID.randomUUID().toString(), "hot-restart");
        file.deleteOnExit();
        return file;
    }

}
