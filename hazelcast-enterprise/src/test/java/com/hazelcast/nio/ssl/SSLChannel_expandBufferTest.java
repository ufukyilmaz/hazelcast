package com.hazelcast.nio.ssl;

import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.ByteBuffer;

import static com.hazelcast.nio.ssl.SSLChannel.expandBufferInReadMode;
import static com.hazelcast.nio.ssl.SSLChannel.expandBufferInWriteMode;
import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class SSLChannel_expandBufferTest {

    @Test
    public void testExpandBufferInWriteMode() {
        byte[] bytes = new byte[]{1, 2, 3, 4};
        ByteBuffer original = ByteBuffer.allocate(4);
        original.put(bytes);

        int originalPos = original.position();

        ByteBuffer expanded = expandBufferInWriteMode(original, bytes.length * 2);

        // the position we write to must not have been changed
        assertEquals(originalPos, expanded.position());
        // the capacity must have increased with the expandFactor
        assertEquals(original.capacity() * 2, expanded.capacity());
        // limit should be capacity
        assertEquals(expanded.capacity(), expanded.limit());

        // make sure all bytes have been copied
        for (int k = 0; k < bytes.length; k++) {
            assertEquals(bytes[k], expanded.get(k));
        }
    }

    @Test
    public void testExpandBufferInReadMode() {
        byte[] bytes = new byte[]{1, 2, 3, 4};
        ByteBuffer original = ByteBuffer.allocate(4);
        original.put(bytes);
        original.flip();

        int originalPos = original.position();

        ByteBuffer expanded = expandBufferInReadMode(original, bytes.length * 2);

        // the position we write to must not have been changed
        assertEquals(originalPos, expanded.position());
        // the capacity must have increased with the expandFactor
        assertEquals(original.capacity() * 2, expanded.capacity());
        // limit should be capacity
        assertEquals(bytes.length, expanded.limit());

        // make sure all bytes have been copied
        for (int k = 0; k < bytes.length; k++) {
            assertEquals(bytes[k], expanded.get(k));
        }
    }
}
