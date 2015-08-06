/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.nio.serialization;

import com.hazelcast.core.PartitionAware;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.memory.PoolingMemoryManager;
import com.hazelcast.nio.serialization.impl.EnterpriseSerializationServiceBuilder;
import com.hazelcast.nio.serialization.impl.HeapData;
import com.hazelcast.nio.serialization.impl.NativeMemoryData;
import com.hazelcast.partition.strategy.DefaultPartitioningStrategy;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.nio.ByteOrder;

import static com.hazelcast.nio.serialization.SerializationConcurrencyTest.Address;
import static com.hazelcast.nio.serialization.SerializationConcurrencyTest.FACTORY_ID;
import static com.hazelcast.nio.serialization.SerializationConcurrencyTest.Person;
import static com.hazelcast.nio.serialization.SerializationConcurrencyTest.PortableAddress;
import static com.hazelcast.nio.serialization.SerializationConcurrencyTest.PortablePerson;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class EnterpriseDataTest {

    private final Person person = new Person(111, 123L, 89.56d, "test-person", new Address("street", 987));

    private final PortablePerson portablePerson = new PortablePerson(222, 456L, "portable-person",
                                                             new PortableAddress("street", 567));

    private EnterpriseSerializationServiceBuilder createSerializationServiceBuilder() {
        final PortableFactory portableFactory = new PortableFactory() {
            public Portable create(int classId) {
                switch (classId) {
                    case 1:
                        return new PortablePerson();
                    case 2:
                        return new PortableAddress();
                }
                throw new IllegalArgumentException();
            }
        };
        return new EnterpriseSerializationServiceBuilder().addPortableFactory(FACTORY_ID, portableFactory);
    }

    @Test
    public void testHeapAndNativeDataEqualityBigEndian() {
        testHeapAndNativeDataEquality(ByteOrder.BIG_ENDIAN, false);
    }

    @Test
    public void testHeapAndNativeDataEqualityLittleEndian() {
        testHeapAndNativeDataEquality(ByteOrder.LITTLE_ENDIAN, false);
    }

    @Test
    public void testHeapAndNativeDataEqualityNativeOrder() {
        testHeapAndNativeDataEquality(ByteOrder.nativeOrder(), false);
    }

    @Test
    public void testHeapAndNativeDataEqualityNativeOrderUsingUnsafe() {
        testHeapAndNativeDataEquality(ByteOrder.nativeOrder(), true);
    }

    private void testHeapAndNativeDataEquality(ByteOrder byteOrder, boolean allowUnsafe) {
        MemoryManager memPool = new PoolingMemoryManager(new MemorySize(8, MemoryUnit.MEGABYTES));
        try {
            EnterpriseSerializationService ss = createSerializationServiceBuilder().setMemoryManager(memPool)
                    .setUseNativeByteOrder(false).setAllowUnsafe(allowUnsafe).setByteOrder(byteOrder)
                    .setPartitioningStrategy(new DefaultPartitioningStrategy())
                    .build();

            Object[] objects = new Object[]{
                    System.currentTimeMillis(),
                    "abcdefghijklmnopqrstuvwxyz 0123456789 !?@#$%&*()[]{}|/<>",
                    person,
                    portablePerson,
                    new PartitionAwareDummyObject(System.nanoTime())
            };

            for (Object object : objects) {
                Data data1 = ss.toData(object, DataType.HEAP);
                Data data2 = ss.toData(object, DataType.NATIVE);

                assertEquals("Types are not matching! Object: "
                                            + object, data1.getType(), data2.getType());
                assertEquals("Sizes are not matching! Object: "
                                            + object, data1.dataSize(), data2.dataSize());
                assertEquals("Hash codes are not matching! Object: "
                                            + object, data1.hashCode(), data2.hashCode());
                assertEquals("Hash64 codes are not matching! Object: "
                                            + object, data1.hash64(), data2.hash64());
                assertEquals("Partition hashes are not matching! Object: "
                                            + object, data1.getPartitionHash(), data2.getPartitionHash());

                assertEquals("Not equal! Object: " + object, data1, data2);  // compare both side of equals
                assertEquals("Not equal! Object: " + object, data2, data1);  // compare both side of equals

                ss.disposeData(data1);
                ss.disposeData(data2);
            }

        } finally {
            memPool.destroy();
        }
    }

    @Test
    public void testDataConversion() {
        testDataConversion(person);
    }

    @Test
    public void testDataConversionPortable() {
        testDataConversion(portablePerson);
    }

    private void testDataConversion(Object object) {
        MemoryManager memPool = new PoolingMemoryManager(new MemorySize(8, MemoryUnit.MEGABYTES));
        try {
            EnterpriseSerializationService ss = createSerializationServiceBuilder().setMemoryManager(memPool)
                    .setUseNativeByteOrder(true).setAllowUnsafe(true).build();

            Data heap = ss.toData(object, DataType.HEAP);
            Data offheap = ss.toData(object, DataType.NATIVE);

            Data heap1 = ss.convertData(heap, DataType.HEAP);
            assertTrue("Type!", heap1 instanceof HeapData);
            assertEquals(heap, heap1);
            assertEquals(offheap, heap1);
            assertTrue("Identity!", heap == heap1);

            Data offheap1 = ss.convertData(heap, DataType.NATIVE);
            assertTrue("Type!", offheap1 instanceof NativeMemoryData);
            assertEquals(heap, offheap1);
            assertEquals(offheap, offheap1);

            Data offheap2 = ss.convertData(offheap, DataType.NATIVE);
            assertTrue("Type!", offheap2 instanceof NativeMemoryData);
            assertEquals(heap, offheap2);
            assertEquals(offheap, offheap2);
            assertTrue("Identity!", offheap == offheap2);

            Data heap2 = ss.convertData(offheap, DataType.HEAP);
            assertTrue("Type!", heap2 instanceof HeapData);
            assertEquals(heap, heap2);
            assertEquals(offheap, heap2);

            ss.disposeData(heap);
            ss.disposeData(heap1);
            ss.disposeData(heap2);
            ss.disposeData(offheap);
            ss.disposeData(offheap1);
            ss.disposeData(offheap2);
        } finally {
            memPool.destroy();
        }
    }

    private static class PartitionAwareDummyObject implements Serializable, PartitionAware {
        private final Object key;

        PartitionAwareDummyObject(Object key) {
            this.key = key;
        }

        @Override
        public Object getPartitionKey() {
            return key;
        }
    }
}
