package com.hazelcast.map.impl.utils;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.ConstructorFunction;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNull;


@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RegistryTest {

    private Registry<Integer, Integer> registry;

    @Before
    public void setUp() throws Exception {
        registry = new SquareRegistry();
    }

    @Test
    public void test_getOrCreate() throws Exception {
        assertEquals(4, registry.getOrCreate(2).intValue());
    }

    @Test
    public void test_getOrNull() throws Exception {
        assertNull(registry.getOrNull(10));
    }

    @Test
    public void test_getAll() throws Exception {
        Map<Integer, Integer> expectedValues = new HashMap<Integer, Integer>();
        for (int i = 0; i < 100; i++) {
            expectedValues.put(i, registry.getOrCreate(i));
        }

        assertEquals(expectedValues, registry.getAll());
    }

    @Test
    public void test_getAll_returnsEmptyMap_whenRegistryIsEmpty() throws Exception {
        assertEquals(Collections.<Integer, Integer>emptyMap(), registry.getAll());
    }


    @Test
    public void test_remove() throws Exception {
        registry.getOrCreate(2);

        assertEquals(4, registry.remove(2).intValue());
    }

    @Test
    public void test_remove_returnsNull_whenRegistryIsEmpty() throws Exception {
        assertNull(registry.remove(12));
    }


    /**
     * Registry class for testing purposes which holds mappings like `integer --> square of integer`.
     */
    private static class SquareRegistry extends AbstractRegistry<Integer, Integer> {

        private static final ConstructorFunction<Integer, Integer> SQUARE = new ConstructorFunction<Integer, Integer>() {
            @Override
            public Integer createNew(Integer key) {
                return key * key;
            }
        };

        @Override
        public ConstructorFunction getConstructorFunction() {
            return SQUARE;
        }
    }
}