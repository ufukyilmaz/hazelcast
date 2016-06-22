package com.hazelcast.spi.hotrestart.impl.io;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class BufferingInputStreamTest {

    private final byte[] mockInput = {1, 2, 3, 4};

    private BufferingInputStream in;

    @Before
    public void before() {
        in = new BufferingInputStream(new ByteArrayInputStream(mockInput));
    }


    @Test
    public void readByteByByte() throws IOException {
        for (byte b : mockInput) {
            assertEquals(b, (byte) in.read());
        }
        assertEquals(-1, in.read());
    }

    @Test
    public void readBufByBuf() throws IOException {
        // Given
        final byte[] buf = new byte[mockInput.length / 2];
        int streamPos = 0;

        // When - Then
        for (int count; (count = in.read(buf)) != -1;) {
            for (int i = 0; i < count; i++) {
                assertEquals(mockInput[streamPos++], buf[i]);
            }
        }
        assertEquals(mockInput.length, streamPos);
    }
}
