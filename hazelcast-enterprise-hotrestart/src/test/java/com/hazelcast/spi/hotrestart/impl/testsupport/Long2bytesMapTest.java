package com.hazelcast.spi.hotrestart.impl.testsupport;

import com.hazelcast.internal.memory.impl.MemoryManagerBean;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.internal.memory.StandardMemoryManager;
import com.hazelcast.spi.hotrestart.RecordDataSink;
import com.hazelcast.spi.hotrestart.impl.testsupport.Long2bytesMap.L2bCursor;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Set;

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;
import static com.hazelcast.memory.MemoryUnit.MEGABYTES;
import static com.hazelcast.spi.hotrestart.impl.testsupport.Long2bytesMap.KEY_SIZE;
import static com.hazelcast.spi.hotrestart.impl.testsupport.MockRecordStoreBase.long2bytes;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class Long2bytesMapTest {

    @Parameters(name = "offHeap == {0}")
    public static Collection<Object[]> params() {
        return asList(new Object[][]{{false}, {true}});
    }

    @Parameter
    public boolean offHeap;

    private final long key1 = 11;
    private final long key2 = 12;
    private final byte[] key1Bytes = long2bytes(key1);
    private final byte[] value1 = {1, 2};
    private final byte[] value2 = {3, 4, 5};
    private final RecordDataHolder holder = new RecordDataHolder();
    private Long2bytesMap map;

    @Before
    public void setup() {
        map = offHeap
                ? new Long2bytesMapOffHeap(new MemoryManagerBean(new StandardMemoryManager(new MemorySize(1, MEGABYTES)), AMEM))
                : new Long2bytesMapOnHeap();
    }

    @After
    public void destroy() {
        map.dispose();
    }

    @Test
    public void whenPutNew_thenGetIt() {
        map.put(key1, value1);
        assertTrue(map.copyEntry(key1, KEY_SIZE + value1.length, holder));
        holder.flip();
        assertContentsEqual(key1Bytes, holder.keyBuffer);
        assertContentsEqual(value1, holder.valueBuffer);
    }

    @Test
    public void sizeShouldBehave() {
        assertEquals(0, map.size());
        map.put(key1, value1);
        assertEquals(1, map.size());
        map.put(key1, value2);
        assertEquals(1, map.size());
        map.put(key2, value1);
        assertEquals(2, map.size());
        map.remove(key1);
        assertEquals(1, map.size());
        map.clear();
        assertEquals(0, map.size());
    }

    @Test
    public void whenRequestNonexisting_thenGetNothing() {
        map.put(key1, value1);
        assertFalse(map.copyEntry(key2, KEY_SIZE, holder));
    }

    @Test
    public void whenReplace_thenGetNew() {
        map.put(key1, value1);
        map.put(key1, value2);
        assertTrue(map.copyEntry(key1, KEY_SIZE + value2.length, holder));
        holder.flip();
        assertContentsEqual(key1Bytes, holder.keyBuffer);
        assertContentsEqual(value2, holder.valueBuffer);
    }

    @Test
    public void keysetShouldBehave() {
        map.put(key1, value1);
        map.put(key1, value2);
        map.put(key2, value1);
        map.remove(key1);
        final Set<Long> keyset = map.keySet();
        assertEquals(1, keyset.size());
        assertFalse(keyset.contains(key1));
        assertTrue(keyset.contains(key2));
    }

    @Test
    public void cursorShouldBehave() {
        map.put(key1, value1);
        map.put(key1, value2);
        map.put(key2, value1);
        map.remove(key1);
        final L2bCursor cursor = map.cursor();
        assertTrue(cursor.advance());
        assertEquals(key2, cursor.key());
        assertEquals(value1.length, cursor.valueSize());
        assertFalse(cursor.advance());
    }

    @Test
    public void clearShouldBehave() {
        map.put(key1, value2);
        map.put(key2, value2);
        assertTrue(map.cursor().advance());
        map.clear();
        assertFalse(map.cursor().advance());
    }

    private static void assertContentsEqual(byte[] expected, ByteBuffer actual) {
        assertEquals(expected.length, actual.remaining());
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], actual.get(i));
        }
    }

    private static final class RecordDataHolder implements RecordDataSink {

        final ByteBuffer keyBuffer = ByteBuffer.allocate(16);
        final ByteBuffer valueBuffer = ByteBuffer.allocate(16);

        @Override
        public ByteBuffer getKeyBuffer(int keySize) {
            assert keyBuffer.remaining() >= keySize;
            return keyBuffer;
        }

        @Override
        public ByteBuffer getValueBuffer(int valueSize) {
            assert valueBuffer.remaining() >= valueSize;
            return valueBuffer;
        }

        void flip() {
            keyBuffer.flip();
            valueBuffer.flip();
        }
    }
}
