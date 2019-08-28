package com.hazelcast.memory;

import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.internal.memory.impl.LibMalloc;
import com.hazelcast.internal.memory.impl.LibMallocFactory;
import com.hazelcast.internal.memory.impl.PersistentMemoryMallocFactory;
import com.hazelcast.internal.memory.impl.UnsafeMallocFactory;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.After;
import org.junit.runners.Parameterized;

import java.util.Collection;

import static com.hazelcast.internal.memory.impl.PersistentMemoryHeap.PERSISTENT_MEMORY_CHECK_DISABLED_PROPERTY;
import static java.util.Arrays.asList;

public class ParameterizedMemoryTest extends HazelcastTestSupport {

    @Parameterized.Parameter
    public boolean persistentMemory;

    @Parameterized.Parameters(name = "persistentMemory: {0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {true},
                {false},
        });
    }

    static LibMallocFactory newLibMallocFactory(boolean persistentMemory) {
        if (persistentMemory) {
            System.setProperty(PERSISTENT_MEMORY_CHECK_DISABLED_PROPERTY, "true");
        }
        if (persistentMemory) {
            NativeMemoryConfig config = new NativeMemoryConfig().setPersistentMemoryDirectory(PERSISTENT_MEMORY_DIRECTORY);
            LibMallocFactory libMallocFactory =  new PersistentMemoryMallocFactory(config);
            return new PersistentMemoryMallocFactoryDelegate(libMallocFactory);
        } else {
            return new UnsafeMallocFactory(new FreeMemoryChecker());
        }
    }

    /**
     * Doubles the size of the allocated heap, because PMDK library
     * utilizes some memory for internal purposes.
     */
    private static class PersistentMemoryMallocFactoryDelegate implements LibMallocFactory {

        private final LibMallocFactory delegate;

        private PersistentMemoryMallocFactoryDelegate(LibMallocFactory delegate) {
            this.delegate = delegate;
        }

        @Override
        public LibMalloc create(long size) {
            return delegate.create(2 * size);
        }
    }

    static LibMalloc newLibMalloc(boolean persistentMemory) {
        LibMallocFactory libMallocFactory = newLibMallocFactory(persistentMemory);
        return libMallocFactory.create(1 << 28);
    }

    @After
    public void cleanUp() {
        System.clearProperty(PERSISTENT_MEMORY_CHECK_DISABLED_PROPERTY);
    }

}
